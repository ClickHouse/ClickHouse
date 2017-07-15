add_definitions(-DHAVE_CONFIG_H=1 -D_XOPEN_SOURCE -D_GNU_SOURCE -Wno-visibility -Wno-header-guard)

include_directories(include include/tdep src)

enable_language(ASM)

add_library(unwind
src/mi/init.c
src/mi/flush_cache.c
src/mi/mempool.c
src/mi/strerror.c
src/x86_64/is_fpreg.c
src/x86_64/regname.c
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
src/dwarf/Lexpr.c
src/dwarf/Lfde.c
src/dwarf/Lfind_proc_info-lsb.c
src/dwarf/Lparser.c
src/dwarf/Lpe.c
src/dwarf/Lstep.c
src/dwarf/global.c
src/elf64.c

src/os-linux.c
src/x86_64/Los-linux.c
)
