#pragma once

#include <cstddef>
#include <memory>
#include <string>
#include <base/types.h>


namespace DB
{

/** A shared-memory region backed by a file (expected to live in tmpfs, e.g. under /dev/shm).
  *
  * The region is created by `mkostemp` (close-on-exec, so the descriptor is not leaked into
  * unrelated `fork`+`exec` children) in the given directory, sized with `ftruncate`, and mapped
  * read-write with `MAP_SHARED`, so a child process that opens the same path and maps it sees the
  * same bytes. Used by executable UDFs to exchange bulk data with the child process without
  * copying it through pipes.
  *
  * The region can be grown in place with `grow` (see below). The backing file descriptor is
  * therefore kept open for the whole lifetime of the region so that growth is a cheap
  * `ftruncate` + remap without reopening the file by path.
  *
  * Design note (file vs anonymous memory): the region is a named tmpfs file rather than an
  * anonymous `memfd_create` descriptor inherited by the child. A `memfd` would be reclaimed by
  * the kernel if the server crashes (no orphaned files) and needs no filesystem access, but a
  * named file is what the feature request specifies and keeps `hugetlbfs`-backed regions easy to
  * adopt later (the child just opens a path). The trade-off is that a hard server crash can leave
  * a stale file under the shared-memory directory. `memfd` remains a documented alternative worth
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
      * The region relies on Linux-only facilities (`mkostemp`, `posix_fallocate`), so it is
      * available on Linux only. Callers that let a user enable the feature should call this at
      * configuration time, so that an unsupported platform is reported once, where the setting is
      * accepted, instead of failing every call at query time. The constructor calls it as well.
      */
    static void checkSupported();

    /// Creates a file `<directory>/clickhouse_udf_shm_XXXXXX` of `size` bytes and maps it.
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

    char * data() { return region_data; }
    const char * data() const { return region_data; }
    size_t size() const { return region_size; }
    const std::string & path() const { return file_path; }

private:
    std::string file_path;
    int region_fd = -1;
    char * region_data = nullptr;
    size_t region_size = 0;
};

using SharedMemoryRegionPtr = std::shared_ptr<SharedMemoryRegion>;

}
