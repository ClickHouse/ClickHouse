extern "C" int dwyu_resolved_function() { return 7; }
extern "C" int (*dwyu_ifunc_resolver())() { return dwyu_resolved_function; }
extern "C" int DWYU_IFUNC_NAME() __attribute__((ifunc("dwyu_ifunc_resolver")));
