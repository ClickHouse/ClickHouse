#pragma once

#include <cstddef>
#include <memory>
#include <string>
#include <sys/types.h>
#include <base/types.h>


namespace DB
{

/** A shared-memory region backed by a file (expected to live in tmpfs, e.g. under /dev/shm).
  *
  * The region is created as an unnamed `O_TMPFILE` (close-on-exec, so the descriptor is not
  * leaked into unrelated `fork`+`exec` children) in a private `0700` subdirectory of the given
  * directory, locked with `flock`, linked as `clickhouse_udf_shm_<random>`, sized with `ftruncate`,
  * and mapped read-write with `MAP_SHARED`, so a child process that opens the same path and maps it
  * sees the same bytes. Used by executable UDFs to exchange bulk data with the child process
  * without copying it through pipes.
  *
  * The region can be resized in place with `grow` and `shrink` (see below). The backing file
  * descriptor is therefore kept open for the whole lifetime of the region so that resizing is a
  * cheap `ftruncate` + remap without reopening the file by path.
  *
  * Design note (file vs anonymous memory): the region is a named tmpfs file rather than an
  * anonymous `memfd_create` descriptor inherited by the child. A `memfd` would be reclaimed by
  * the kernel if the server crashes (no orphaned files) and needs no filesystem access, but a
  * named file is what the feature request specifies and keeps `hugetlbfs`-backed regions easy to
  * adopt later (the child just opens a path). The trade-off is that a hard server crash can leave
  * a stale file under the shared-memory directory, still holding its pages; such files are
  * reclaimed periodically while regions are created in the same directory - the exclusive `flock`
  * a live region holds is what distinguishes them. `memfd` remains a documented alternative worth
  * revisiting.
  *
  * A `memfd` is also the only way to make the region impossible for the command to shrink, which a
  * named file cannot be: sealing is available only for a `memfd` created with `MFD_ALLOW_SEALING`,
  * and a `memfd` cannot be linked into the shared-memory directory - it lives on its own
  * filesystem, so `linkat` reports `EXDEV`. This matters because the command opens the region for
  * writing and can therefore resize it, while the server maps it: a file that gets shorter makes
  * the server fault (`SIGBUS`). The consumer compares the file against the mapping before it
  * touches the region (see `backingFileSize`), which catches a command that damages the region and
  * then answers, but not one that truncates it in the instant between that check and the access.
  * Adopting `memfd` to close that too means, at least:
  *   - sealing with `F_SEAL_SHRINK | F_SEAL_SEAL`: shrinking is what has to be denied, and sealing
  *     the seal set keeps the command - which holds a writable descriptor - from adding
  *     `F_SEAL_GROW` itself and breaking the server's own growth;
  *   - handing the descriptor to the command instead of a path: it has to survive `exec` (so no
  *     `FD_CLOEXEC` on it) and is named `/proc/self/fd/N` in the request, which leaves the
  *     command's side of the protocol (`open` the path, `mmap` it) unchanged;
  *   - deciding the lifetime of a pooled region up front. A sealed region cannot be shrunk at all,
  *     and recreating one does not reach a worker that is already running: it keeps the descriptor
  *     it inherited. So either a region is never shrunk while its worker lives (giving up the trim
  *     back to `shared_memory_size`), or the worker is restarted when the region should shrink, or
  *     a replacement descriptor is delivered to the running worker - which needs a control channel
  *     that can carry descriptors (`SCM_RIGHTS` over a unix socket), that is, a different protocol.
  *
  * Passing descriptors over a unix socket, or giving up the mapping for `pread`/`pwrite`, would
  * close the same hole; the first needs that protocol change as well, and the second removes the
  * copy-free exchange this transport exists for.
  *
  * This class only owns the mapping and the file; it does no memory accounting, because the
  * mapping can outlive a single query (it is reused across `executable_pool` borrows). The
  * consumer charges the query memory tracker per borrow. The destructor unmaps and unlinks
  * the file.
  */
class SharedMemoryRegion
{
public:
    /** Throws `NOT_IMPLEMENTED` if this platform cannot back a shared-memory region.
      *
      * The region relies on Linux-only facilities (`O_TMPFILE`, `posix_fallocate`), so it is
      * available on Linux only. Callers that let a user enable the feature should call this at
      * configuration time, so that an unsupported platform is reported once, where the setting is
      * accepted, instead of failing every call at query time. The constructor calls it as well.
      */
    static void checkSupported();

    /// Also creates and verifies the private region subdirectory, checks that the filesystem
    /// supports creating and linking an unnamed region file, and verifies that `/proc/self/fd` is
    /// available. Intended for validating UDF configuration at load time.
    static void checkSupported(const std::string & directory);

    /// Creates a file `<directory>/.clickhouse-udf-shared-memory/clickhouse_udf_shm_<random>` of
    /// `size` bytes and maps it. Also periodically reclaims the region files that a previous server
    /// left in that private subdirectory by dying without running destructors.
    SharedMemoryRegion(const std::string & directory, size_t size);

    ~SharedMemoryRegion();

    SharedMemoryRegion(const SharedMemoryRegion &) = delete;
    SharedMemoryRegion & operator=(const SharedMemoryRegion &) = delete;
    SharedMemoryRegion(SharedMemoryRegion &&) = delete;
    SharedMemoryRegion & operator=(SharedMemoryRegion &&) = delete;

    /** Grows the region to `new_size` bytes (must be strictly greater than the current size).
      *
      * The backing file is extended with `ftruncate` and the mapping is replaced, so `data`
      * may return a different pointer afterwards; callers must re-read `data` after growing.
      * Because the file path does not change, a child process that re-maps the file on the next
      * request (as the reference client does, mapping the whole file with `mmap(fd, 0)`)
      * transparently observes the larger region. On failure the region is left unchanged and an
      * exception is thrown.
      */
    void grow(size_t new_size);

    /** Shrinks the region to `new_size` bytes (must be greater than zero and strictly less than
      * the current size), releasing the backing pages beyond it.
      *
      * Like `grow`, this replaces the mapping, so `data` may return a different pointer, and the
      * bytes beyond `new_size` are lost. Runs on teardown paths (a pooled region is trimmed back
      * to its configured size when a borrow ends), where an exception has nowhere to go: a failure
      * is logged instead and leaves the region usable. The backing file is never left larger than
      * `size` reports, because that is what the consumer charges its memory trackers for.
      */
    void shrink(size_t new_size) noexcept;

    char * data() { return region_data; }
    const char * data() const { return region_data; }
    size_t size() const { return region_size; }
    const std::string & path() const { return file_path; }

    /** State of the file behind the mapping, which the command can change behind the server's back:
      * it opens the region by path and needs write access to it, so it can resize the file, unlink
      * it or replace it with another one.
      *
      * `size` is the current length of the file the server has open. `path_is_ours` says that the
      * path still names exactly that file (same device and inode, still linked); it is false once
      * the file was unlinked or the name was taken over by something else.
      *
      * The consumer checks both before it uses a region: a file shorter than the mapping makes the
      * server fault (`SIGBUS`) on pages the file no longer backs, a longer one holds memory nobody
      * accounts for, and a path that no longer leads to this file would be handed to the command in
      * the next request. Throws if the file cannot be inspected.
      */
    struct BackingFileState
    {
        size_t size;
        bool path_is_ours;
    };

    BackingFileState backingFileState() const;

    /** Copies `size` bytes at `offset` out of the backing file into `destination`, without touching
      * the mapping.
      *
      * The command holds the region open for writing for as long as it runs, so it can truncate the
      * file at any moment - including in the instant between the check that the file is still whole
      * and the server's use of the bytes it just reported. Reading those bytes through the mapping
      * would fault (`SIGBUS`) on pages the file no longer backs, and that fault cannot be caught:
      * it takes the whole server down, along with every other query on it. A `pread` of the same
      * range answers a truncated file with a short read instead - an ordinary error, which fails the
      * one query whose command caused it.
      *
      * This is why the result is copied out rather than parsed where it lies. It costs one copy of
      * the output; the input is still placed into the region without one. Note that the input side
      * keeps the corresponding hazard, because the server writes it through the mapping - see the
      * design note above.
      *
      * Throws if the file cannot be read, or if it no longer holds `size` bytes at `offset`.
      */
    void readBackingFile(char * destination, size_t offset, size_t size) const;

private:
    std::string file_path;
    int region_fd = -1;
    char * region_data = nullptr;
    size_t region_size = 0;
    /// Identity of the file this region owns, taken when it was created: the path may later name
    /// something else, and neither the size check nor `unlink` in the destructor may act on it then.
    dev_t file_device = 0;
    ino_t file_inode = 0;
    /// Length of the current mapping, which is what `munmap` must be given. It equals `region_size`
    /// except after a `shrink` whose remap failed: the region then keeps the oversized mapping and
    /// uses only its prefix, because the bytes beyond it are no longer backed by the file.
    size_t mapped_size = 0;
};

using SharedMemoryRegionPtr = std::shared_ptr<SharedMemoryRegion>;

}
