if (NOT OS_DARWIN)
    message (FATAL_ERROR "darwin-compatibility can only be used on Darwin")
endif ()

if (SANITIZE STREQUAL "thread")
    # TSan interposes pthread_rwlock_* itself to track lock order, so a second interposition
    # would either lose those annotations or fight over the symbols. Only TSan does this:
    # ASan, MSan and UBSan leave these functions alone, and since they keep the query
    # profiler enabled (see sanitize.cmake), they need the workaround as much as release
    # builds do.
    message (FATAL_ERROR "darwin-compatibility must not be linked into TSan builds")
endif ()

add_library(darwin-compatibility STATIC pthread_rwlock_shim.c)

# The shim must be force-loaded, and must be the ONLY link entry for this archive:
# - Lazy archive scanning cannot be relied on: `Threads::Threads` puts -lpthread in the
#   middle of the link line, and once ld64 processes that dylib, every outstanding
#   pthread_rwlock_* undefined (including a -u seed) binds to libSystem, so a normal
#   archive entry after it is never fetched. A force-loaded object's definitions, by
#   contrast, override earlier dylib bindings.
# - A normal entry alongside -force_load is a duplicate-symbol error in links where a
#   pthread_rwlock reference does reach the archive entry first (spelled with a different
#   path, ld64.lld loads the member twice; spelled identically, it skips the force load).
# The flag is a link-libraries entry (not a link option) with a literal path (not a
# generator expression) because the root CMakeLists propagates global-libs into
# global-group via $<TARGET_PROPERTY:global-libs,INTERFACE_LINK_LIBRARIES>, which drops
# INTERFACE_LINK_OPTIONS and does not evaluate generator expressions.
# The stub archive exists only to make consumers depend on this directory's build, since
# a bare linker flag carries no target dependency.
add_library(darwin-compatibility-dep STATIC dep_anchor.c)
add_dependencies(darwin-compatibility-dep darwin-compatibility)
target_link_libraries(global-libs INTERFACE
    darwin-compatibility-dep
    "-Wl,-force_load,${CMAKE_CURRENT_BINARY_DIR}/libdarwin-compatibility.a")

message (STATUS "pthread_rwlock will be replaced to work around a lost wakeup in Darwin psynch (FB24027930)")
