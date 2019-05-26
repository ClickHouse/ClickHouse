enable_language(ASM)

if (ARCH_PPC64LE)
    add_library(unwind
    src/mi/init.c
    src/mi/flush_cache.c
    src/mi/mempool.c
    src/mi/strerror.c
    src/mi/_ReadULEB.c
    src/mi/_ReadSLEB.c
    src/mi/backtrace.c
    src/mi/dyn-cancel.c
    src/mi/dyn-info-list.c
    src/mi/dyn-register.c
    src/mi/Ldyn-extract.c
    src/mi/Lfind_dynamic_proc_info.c
    src/mi/Lget_accessors.c
    src/mi/Lget_proc_info_by_ip.c
    src/mi/Lget_proc_name.c
    src/mi/Lput_dynamic_unwind_info.c
    src/mi/Ldestroy_addr_space.c
    src/mi/Lget_reg.c
    src/mi/Lset_reg.c
    src/mi/Lget_fpreg.c
    src/mi/Lset_fpreg.c
    src/mi/Lset_caching_policy.c
    src/dwarf/Lexpr.c
    src/dwarf/Lfde.c
    src/dwarf/Lfind_proc_info-lsb.c
    src/dwarf/Lparser.c
    src/dwarf/Lpe.c
    src/dwarf/global.c
    src/elf64.c
    src/os-linux.c

    src/ppc64/is_fpreg.c
    src/ppc64/regname.c
    src/ppc64/get_func_addr.c
    src/ppc/Linit_local.c
    src/ppc/Linit_remote.c
    src/ppc/Lis_signal_frame.c
    src/ppc/longjmp.S
    src/ppc/Lreg_states_iterate.c
    src/ppc/siglongjmp.S
    src/ppc64/setcontext.S
    src/ppc64/Lcreate_addr_space.c
    src/ppc64/Lglobal.c
    src/ppc64/Linit.c
    src/ppc64/Lreg_states_iterate.c
    src/ppc64/Lregs.c
    src/ppc64/Lresume.c
    src/ppc64/Lstep.c
    src/ppc64/regname.c
    src/ppc64/setcontext.S
    )
else ()
    add_library(unwind
    src/mi/init.c
    src/mi/flush_cache.c
    src/mi/mempool.c
    src/mi/strerror.c
    src/mi/_ReadULEB.c
    src/mi/_ReadSLEB.c
    src/mi/backtrace.c
    src/mi/dyn-cancel.c
    src/mi/dyn-info-list.c
    src/mi/dyn-register.c
    src/mi/Ldyn-extract.c
    src/mi/Lfind_dynamic_proc_info.c
    src/mi/Lget_accessors.c
    src/mi/Lget_proc_info_by_ip.c
    src/mi/Lget_proc_name.c
    src/mi/Lput_dynamic_unwind_info.c
    src/mi/Ldestroy_addr_space.c
    src/mi/Lget_reg.c
    src/mi/Lset_reg.c
    src/mi/Lget_fpreg.c
    src/mi/Lset_fpreg.c
    src/mi/Lset_caching_policy.c
    src/dwarf/Lexpr.c
    src/dwarf/Lfde.c
    src/dwarf/Lfind_proc_info-lsb.c
    src/dwarf/Lparser.c
    src/dwarf/Lpe.c
    src/dwarf/global.c
    src/elf64.c
    src/os-linux.c

    src/x86_64/is_fpreg.c
    src/x86_64/regname.c
    src/x86_64/setcontext.S
    src/x86_64/Lcreate_addr_space.c
    src/x86_64/Lget_save_loc.c
    src/x86_64/Lglobal.c
    src/x86_64/Linit.c
    src/x86_64/Linit_local.c
    src/x86_64/Linit_remote.c
    src/x86_64/Lget_proc_info.c
    src/x86_64/Lregs.c
    src/x86_64/Lresume.c
    src/x86_64/Lstash_frame.c
    src/x86_64/Lstep.c
    src/x86_64/Ltrace.c
    src/x86_64/getcontext.S
    src/x86_64/Los-linux.c
    )
endif()

find_file (HAVE_ATOMIC_OPS_H "atomic_ops.h")
configure_file (config/config.h.in ${CMAKE_CURRENT_BINARY_DIR}/config/config.h)
configure_file (config/libunwind.h.in ${CMAKE_CURRENT_BINARY_DIR}/config/libunwind.h)
configure_file (config/libunwind-common.h.in ${CMAKE_CURRENT_BINARY_DIR}/config/libunwind-common.h)
configure_file (config/tdep/libunwind_i.h.in ${CMAKE_CURRENT_BINARY_DIR}/config/tdep/libunwind_i.h)

target_compile_definitions (unwind PRIVATE HAVE_CONFIG_H=1 _XOPEN_SOURCE _GNU_SOURCE)
target_compile_options (unwind PRIVATE -Wno-visibility -Wno-header-guard)

target_include_directories (unwind PUBLIC include)
target_include_directories (unwind PRIVATE include/tdep)
target_include_directories (unwind PUBLIC ${CMAKE_CURRENT_BINARY_DIR}/config)
target_include_directories (unwind PRIVATE ${CMAKE_CURRENT_BINARY_DIR}/config/tdep)
target_include_directories (unwind PRIVATE src)
