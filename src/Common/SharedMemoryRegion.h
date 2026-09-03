#pragma once

#include <cstddef>
#include <memory>
#include <string>
#include <base/types.h>


namespace DB
{

/** A shared-memory region backed by a file (expected to live in tmpfs, e.g. under /dev/shm).
  *
  * The region is created as an unnamed `O_TMPFILE` (close-on-exec, so the descriptor is not
  * leaked into unrelated `fork`+`exec` children) in the given directory, locked with `flock`,
  * linked into the directory as `clickhouse_udf_shm_<random>`, sized with `ftruncate`, and mapped
  * read-write with `MAP_SHARED`, so a child process that opens the same path and maps it sees the
  * same bytes. Used by executable UDFs to exchange bulk data with the child process without
  * copying it through pipes.
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

    /// Also verifies that `directory` has safe permissions, supports creating and linking an
    /// unnamed region file, and that `/proc/self/fd` is available. Intended for validating UDF
    /// configuration at load time.
    static void checkSupported(const std::string & directory);

    /// Creates a file `<directory>/clickhouse_udf_shm_<random>` of `size` bytes and maps it.
    /// Also periodically reclaims the region files that a previous server left in `directory` by
    /// dying without running destructors.
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

private:
    std::string file_path;
    int region_fd = -1;
    char * region_data = nullptr;
    size_t region_size = 0;
    /// Length of the current mapping, which is what `munmap` must be given. It equals `region_size`
    /// except after a `shrink` whose remap failed: the region then keeps the oversized mapping and
    /// uses only its prefix, because the bytes beyond it are no longer backed by the file.
    size_t mapped_size = 0;
};

using SharedMemoryRegionPtr = std::shared_ptr<SharedMemoryRegion>;

}
