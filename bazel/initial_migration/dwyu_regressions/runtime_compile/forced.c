#if !defined(DWYU_RUNTIME_FIXTURE_VALUE)
#error Runtime fixture requires the compiler forced include.
#endif

int runtime_fixture_forced(void)
{
    return DWYU_RUNTIME_FIXTURE_VALUE;
}
