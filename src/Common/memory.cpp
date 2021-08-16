#if defined(OS_DARWIN) && defined(BUNDLED_STATIC_JEMALLOC)

extern "C"
{
    extern void zone_register();
}

struct InitializeJemallocZoneAllocatorForOSX
{
    InitializeJemallocZoneAllocatorForOSX()
    {
        /// In case of OSX jemalloc register itself as a default zone allocator.
        ///
        /// But when you link statically then zone_register() will not be called,
        /// and even will be optimized out:
        ///
        /// It is ok to call it twice (i.e. in case of shared libraries)
        /// Since zone_register() is a no-op if the default zone is already replaced with something.
        ///
        /// https://github.com/jemalloc/jemalloc/issues/708
        zone_register();
    }
} initializeJemallocZoneAllocatorForOSX;

#endif
