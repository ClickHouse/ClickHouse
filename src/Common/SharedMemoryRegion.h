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
  * the server fault (`SIGBUS`), which is fatal for the whole server and cannot be caught.
  *
  * The transport that uses this class does not rely on winning that race, and does not rely on the
  * checks either: it never touches the mapping. Both directions go through the descriptor
  * (`readBackingFile`, `writeBackingFile`), where a file the command shortened is an ordinary
  * error - or, for a write, simply extended again - instead of a fatal signal. `backingFileState`
  * remains, but for what it can actually answer: whether the region still costs what it is charged
  * for, and whether its path still leads to it. What a sealed region would add is not safety any
  * more but the copy: with sealing, the server could serialize straight into the mapping and parse
  * straight out of it. Adopting `memfd` for that means, at least:
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
  * Passing descriptors over a unix socket needs that protocol change as well. Giving up the
  * mapping for `pread`/`pwrite` is what the transport does today: it costs one copy per direction
  * on the server side - the command's side stays copy-free, and the exchange still avoids the two
  * copies and the per-64-KiB syscalls of the pipe transport it replaces.
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

    /** Puts the backing file back to `size()` if it has grown past it, and reports how many bytes
      * it actually holds afterwards.
      *
      * For the moment a region stops being used and starts merely existing - a pooled worker going
      * back into the pool. Its bytes are charged to the server-wide tracker for that time, and the
      * number charged has to be the number of pages the `tmpfs` is really holding, not the number
      * this object believes it asked for. The two can differ: a command can `ftruncate` the file it
      * was given, and a `grow` whose rollback failed leaves the file longer than the region. Pages
      * beyond what is charged are held by nobody and counted by nobody.
      *
      * Truncating back is what makes the charge true rather than merely accurate - it gives the
      * pages back. Only growth is repaired: a file that got *shorter* is the dangerous direction
      * and belongs to `backingFileState` and the caller's integrity check, not here, and extending
      * it again would only paper over that.
      *
      * When the file cannot be read or cannot be put back, the size actually on disk is returned,
      * so the caller charges for what exists. Undercounting is the one answer not available.
      *
      * This is a snapshot, not a guarantee: the command holds the file open and can resize it again
      * the moment this returns. What that costs is bounded by the next borrow, which checks the
      * region before it uses it and discards a worker that damaged it.
      */
    size_t reconcileBackingFileSize() noexcept;

    /** The mapping. Only for a consumer that is the sole writer of the file behind it.
      *
      * The executable-UDF transport deliberately does not use this: the command holds the same file
      * open for writing and can shorten it at any moment, and touching a page the file no longer
      * backs raises `SIGBUS`, which cannot be caught and takes the whole server down. Both
      * directions of that exchange go through `readBackingFile` / `writeBackingFile` instead, where
      * a shortened file is an ordinary error. Do not reintroduce a mapped access there.
      */
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
      * This is why the result is copied out rather than parsed where it lies, and why the input is
      * placed into the region the same way - see `writeBackingFile`.
      *
      * Throws if the file cannot be read, or if it no longer holds `size` bytes at `offset`.
      */
    void readBackingFile(char * destination, size_t offset, size_t size) const;

    /** Copies `size` bytes from `source` into the backing file at `offset`, without touching the
      * mapping. The counterpart of `readBackingFile`, and there for the same reason.
      *
      * Writing the input through the mapping would carry exactly the hazard `readBackingFile`
      * avoids for the output, and one that no check can close: the command can shorten the file
      * between the moment the server verifies it and the moment the server's store lands on a page
      * the file no longer backs, and that `SIGBUS` is fatal for the whole server. `pwrite` has no
      * such window - a file the command shortened is simply extended again by the write, or the
      * write fails with an ordinary `errno` - so the transport does not depend on winning a race
      * with the process on the other end.
      *
      * `offset + size` must fit into `size()`; the file is not consulted for that bound, because
      * the file is what cannot be trusted here.
      *
      * Throws if the file cannot be written.
      */
    void writeBackingFile(const char * source, size_t offset, size_t size);

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
