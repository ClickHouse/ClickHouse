# Vectorscan is drop-in replacement for Hyperscan.
if ((ARCH_AMD64 AND NOT NO_SSE3_OR_HIGHER) OR ARCH_AARCH64)
    option (ENABLE_VECTORSCAN "Enable vectorscan" ${ENABLE_LIBRARIES})
endif()

if (NOT ENABLE_VECTORSCAN)
    message (STATUS "Not using vectorscan")
    return()
endif()

set (LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/vectorscan")

# Gobble up all c/cpp files in vectorscan/src/, omit *dump*.c/cpp files as we don't use the dump feature (see x86/config.h)
set (SRCS
    "${LIBRARY_DIR}/src/alloc.c"
    "${LIBRARY_DIR}/src/crc32.c"
    "${LIBRARY_DIR}/src/database.c"
    # "${LIBRARY_DIR}/src/dispatcher.c" # the linker's wrath be upon those who include dispatcher.c.
    "${LIBRARY_DIR}/src/grey.cpp"
    "${LIBRARY_DIR}/src/hs.cpp"
    "${LIBRARY_DIR}/src/hs_valid_platform.c"
    "${LIBRARY_DIR}/src/hs_version.c"
    "${LIBRARY_DIR}/src/runtime.c"
    "${LIBRARY_DIR}/src/scratch.c"
    "${LIBRARY_DIR}/src/stream_compress.c"

    "${LIBRARY_DIR}/src/compiler/asserts.cpp"
    "${LIBRARY_DIR}/src/compiler/compiler.cpp"
    "${LIBRARY_DIR}/src/compiler/error.cpp"

    "${LIBRARY_DIR}/src/fdr/engine_description.cpp"
    "${LIBRARY_DIR}/src/fdr/fdr.c"
    "${LIBRARY_DIR}/src/fdr/fdr_compile.cpp"
    "${LIBRARY_DIR}/src/fdr/fdr_compile_util.cpp"
    "${LIBRARY_DIR}/src/fdr/fdr_confirm_compile.cpp"
    "${LIBRARY_DIR}/src/fdr/fdr_engine_description.cpp"
    "${LIBRARY_DIR}/src/fdr/flood_compile.cpp"
    "${LIBRARY_DIR}/src/fdr/teddy.c"
    "${LIBRARY_DIR}/src/fdr/teddy_avx2.c"
    "${LIBRARY_DIR}/src/fdr/teddy_compile.cpp"
    "${LIBRARY_DIR}/src/fdr/teddy_engine_description.cpp"

    "${LIBRARY_DIR}/src/hwlm/hwlm.c"
    "${LIBRARY_DIR}/src/hwlm/hwlm_build.cpp"
    "${LIBRARY_DIR}/src/hwlm/hwlm_literal.cpp"
    "${LIBRARY_DIR}/src/hwlm/noodle_build.cpp"
    "${LIBRARY_DIR}/src/hwlm/noodle_engine.cpp"

    "${LIBRARY_DIR}/src/nfa/accel.c"
    "${LIBRARY_DIR}/src/nfa/accel_dfa_build_strat.cpp"
    "${LIBRARY_DIR}/src/nfa/accelcompile.cpp"
    "${LIBRARY_DIR}/src/nfa/castle.c"
    "${LIBRARY_DIR}/src/nfa/castlecompile.cpp"
    "${LIBRARY_DIR}/src/nfa/dfa_build_strat.cpp"
    "${LIBRARY_DIR}/src/nfa/dfa_min.cpp"
    "${LIBRARY_DIR}/src/nfa/gough.c"
    "${LIBRARY_DIR}/src/nfa/goughcompile.cpp"
    "${LIBRARY_DIR}/src/nfa/goughcompile_accel.cpp"
    "${LIBRARY_DIR}/src/nfa/goughcompile_reg.cpp"
    "${LIBRARY_DIR}/src/nfa/lbr.c"
    "${LIBRARY_DIR}/src/nfa/limex_64.c"
    "${LIBRARY_DIR}/src/nfa/limex_accel.c"
    "${LIBRARY_DIR}/src/nfa/limex_compile.cpp"
    "${LIBRARY_DIR}/src/nfa/limex_native.c"
    "${LIBRARY_DIR}/src/nfa/limex_simd128.c"
    "${LIBRARY_DIR}/src/nfa/limex_simd256.c"
    "${LIBRARY_DIR}/src/nfa/limex_simd384.c"
    "${LIBRARY_DIR}/src/nfa/limex_simd512.c"
    "${LIBRARY_DIR}/src/nfa/mcclellan.c"
    "${LIBRARY_DIR}/src/nfa/mcclellancompile.cpp"
    "${LIBRARY_DIR}/src/nfa/mcclellancompile_util.cpp"
    "${LIBRARY_DIR}/src/nfa/mcsheng.c"
    "${LIBRARY_DIR}/src/nfa/mcsheng_compile.cpp"
    "${LIBRARY_DIR}/src/nfa/mcsheng_data.c"
    "${LIBRARY_DIR}/src/nfa/mpv.c"
    "${LIBRARY_DIR}/src/nfa/mpvcompile.cpp"
    "${LIBRARY_DIR}/src/nfa/nfa_api_dispatch.c"
    "${LIBRARY_DIR}/src/nfa/nfa_build_util.cpp"
    "${LIBRARY_DIR}/src/nfa/rdfa.cpp"
    "${LIBRARY_DIR}/src/nfa/rdfa_graph.cpp"
    "${LIBRARY_DIR}/src/nfa/rdfa_merge.cpp"
    "${LIBRARY_DIR}/src/nfa/repeat.c"
    "${LIBRARY_DIR}/src/nfa/repeatcompile.cpp"
    "${LIBRARY_DIR}/src/nfa/sheng.c"
    "${LIBRARY_DIR}/src/nfa/shengcompile.cpp"
    "${LIBRARY_DIR}/src/nfa/shufti.cpp"
    "${LIBRARY_DIR}/src/nfa/shufticompile.cpp"
    "${LIBRARY_DIR}/src/nfa/tamarama.c"
    "${LIBRARY_DIR}/src/nfa/tamaramacompile.cpp"
    "${LIBRARY_DIR}/src/nfa/truffle.cpp"
    "${LIBRARY_DIR}/src/nfa/trufflecompile.cpp"
    "${LIBRARY_DIR}/src/nfa/vermicelli_simd.cpp"
    "${LIBRARY_DIR}/src/nfa/vermicellicompile.cpp"

    "${LIBRARY_DIR}/src/nfagraph/ng.cpp"
    "${LIBRARY_DIR}/src/nfagraph/ng_anchored_acyclic.cpp"
    "${LIBRARY_DIR}/src/nfagraph/ng_anchored_dots.cpp"
    "${LIBRARY_DIR}/src/nfagraph/ng_asserts.cpp"
    "${LIBRARY_DIR}/src/nfagraph/ng_builder.cpp"
    "${LIBRARY_DIR}/src/nfagraph/ng_calc_components.cpp"
    "${LIBRARY_DIR}/src/nfagraph/ng_cyclic_redundancy.cpp"
    "${LIBRARY_DIR}/src/nfagraph/ng_depth.cpp"
    "${LIBRARY_DIR}/src/nfagraph/ng_dominators.cpp"
    "${LIBRARY_DIR}/src/nfagraph/ng_edge_redundancy.cpp"
    "${LIBRARY_DIR}/src/nfagraph/ng_equivalence.cpp"
    "${LIBRARY_DIR}/src/nfagraph/ng_execute.cpp"
    "${LIBRARY_DIR}/src/nfagraph/ng_expr_info.cpp"
    "${LIBRARY_DIR}/src/nfagraph/ng_extparam.cpp"
    "${LIBRARY_DIR}/src/nfagraph/ng_fixed_width.cpp"
    "${LIBRARY_DIR}/src/nfagraph/ng_fuzzy.cpp"
    "${LIBRARY_DIR}/src/nfagraph/ng_haig.cpp"
    "${LIBRARY_DIR}/src/nfagraph/ng_holder.cpp"
    "${LIBRARY_DIR}/src/nfagraph/ng_is_equal.cpp"
    "${LIBRARY_DIR}/src/nfagraph/ng_lbr.cpp"
    "${LIBRARY_DIR}/src/nfagraph/ng_limex.cpp"
    "${LIBRARY_DIR}/src/nfagraph/ng_limex_accel.cpp"
    "${LIBRARY_DIR}/src/nfagraph/ng_literal_analysis.cpp"
    "${LIBRARY_DIR}/src/nfagraph/ng_literal_component.cpp"
    "${LIBRARY_DIR}/src/nfagraph/ng_literal_decorated.cpp"
    "${LIBRARY_DIR}/src/nfagraph/ng_mcclellan.cpp"
    "${LIBRARY_DIR}/src/nfagraph/ng_misc_opt.cpp"
    "${LIBRARY_DIR}/src/nfagraph/ng_netflow.cpp"
    "${LIBRARY_DIR}/src/nfagraph/ng_prefilter.cpp"
    "${LIBRARY_DIR}/src/nfagraph/ng_prune.cpp"
    "${LIBRARY_DIR}/src/nfagraph/ng_puff.cpp"
    "${LIBRARY_DIR}/src/nfagraph/ng_redundancy.cpp"
    "${LIBRARY_DIR}/src/nfagraph/ng_region.cpp"
    "${LIBRARY_DIR}/src/nfagraph/ng_region_redundancy.cpp"
    "${LIBRARY_DIR}/src/nfagraph/ng_repeat.cpp"
    "${LIBRARY_DIR}/src/nfagraph/ng_reports.cpp"
    "${LIBRARY_DIR}/src/nfagraph/ng_restructuring.cpp"
    "${LIBRARY_DIR}/src/nfagraph/ng_revacc.cpp"
    "${LIBRARY_DIR}/src/nfagraph/ng_sep.cpp"
    "${LIBRARY_DIR}/src/nfagraph/ng_small_literal_set.cpp"
    "${LIBRARY_DIR}/src/nfagraph/ng_som.cpp"
    "${LIBRARY_DIR}/src/nfagraph/ng_som_add_redundancy.cpp"
    "${LIBRARY_DIR}/src/nfagraph/ng_som_util.cpp"
    "${LIBRARY_DIR}/src/nfagraph/ng_split.cpp"
    "${LIBRARY_DIR}/src/nfagraph/ng_squash.cpp"
    "${LIBRARY_DIR}/src/nfagraph/ng_stop.cpp"
    "${LIBRARY_DIR}/src/nfagraph/ng_uncalc_components.cpp"
    "${LIBRARY_DIR}/src/nfagraph/ng_utf8.cpp"
    "${LIBRARY_DIR}/src/nfagraph/ng_util.cpp"
    "${LIBRARY_DIR}/src/nfagraph/ng_vacuous.cpp"
    "${LIBRARY_DIR}/src/nfagraph/ng_violet.cpp"
    "${LIBRARY_DIR}/src/nfagraph/ng_width.cpp"

    "${LIBRARY_DIR}/src/parser/AsciiComponentClass.cpp"
    "${LIBRARY_DIR}/src/parser/Component.cpp"
    "${LIBRARY_DIR}/src/parser/ComponentAlternation.cpp"
    "${LIBRARY_DIR}/src/parser/ComponentAssertion.cpp"
    "${LIBRARY_DIR}/src/parser/ComponentAtomicGroup.cpp"
    "${LIBRARY_DIR}/src/parser/ComponentBackReference.cpp"
    "${LIBRARY_DIR}/src/parser/ComponentBoundary.cpp"
    "${LIBRARY_DIR}/src/parser/ComponentByte.cpp"
    "${LIBRARY_DIR}/src/parser/ComponentClass.cpp"
    "${LIBRARY_DIR}/src/parser/ComponentCondReference.cpp"
    "${LIBRARY_DIR}/src/parser/ComponentEUS.cpp"
    "${LIBRARY_DIR}/src/parser/ComponentEmpty.cpp"
    "${LIBRARY_DIR}/src/parser/ComponentRepeat.cpp"
    "${LIBRARY_DIR}/src/parser/ComponentSequence.cpp"
    "${LIBRARY_DIR}/src/parser/ComponentVisitor.cpp"
    "${LIBRARY_DIR}/src/parser/ComponentWordBoundary.cpp"
    "${LIBRARY_DIR}/src/parser/ConstComponentVisitor.cpp"
    "${LIBRARY_DIR}/src/parser/Utf8ComponentClass.cpp"
    "${LIBRARY_DIR}/src/parser/buildstate.cpp"
    "${LIBRARY_DIR}/src/parser/buildstate.cpp"
    "${LIBRARY_DIR}/src/parser/check_refs.cpp"
    "${LIBRARY_DIR}/src/parser/check_refs.cpp"
    "${LIBRARY_DIR}/src/parser/logical_combination.cpp"
    "${LIBRARY_DIR}/src/parser/parse_error.cpp"
    "${LIBRARY_DIR}/src/parser/parser_util.cpp"
    "${LIBRARY_DIR}/src/parser/prefilter.cpp"
    "${LIBRARY_DIR}/src/parser/shortcut_literal.cpp"
    "${LIBRARY_DIR}/src/parser/ucp_table.cpp"
    "${LIBRARY_DIR}/src/parser/unsupported.cpp"
    "${LIBRARY_DIR}/src/parser/utf8_validate.cpp"

    "${LIBRARY_DIR}/src/rose/block.c"
    "${LIBRARY_DIR}/src/rose/catchup.c"
    "${LIBRARY_DIR}/src/rose/init.c"
    "${LIBRARY_DIR}/src/rose/match.c"
    "${LIBRARY_DIR}/src/rose/program_runtime.c"
    "${LIBRARY_DIR}/src/rose/rose_build_add.cpp"
    "${LIBRARY_DIR}/src/rose/rose_build_add_mask.cpp"
    "${LIBRARY_DIR}/src/rose/rose_build_anchored.cpp"
    "${LIBRARY_DIR}/src/rose/rose_build_bytecode.cpp"
    "${LIBRARY_DIR}/src/rose/rose_build_castle.cpp"
    "${LIBRARY_DIR}/src/rose/rose_build_compile.cpp"
    "${LIBRARY_DIR}/src/rose/rose_build_convert.cpp"
    "${LIBRARY_DIR}/src/rose/rose_build_dedupe.cpp"
    "${LIBRARY_DIR}/src/rose/rose_build_engine_blob.cpp"
    "${LIBRARY_DIR}/src/rose/rose_build_exclusive.cpp"
    "${LIBRARY_DIR}/src/rose/rose_build_groups.cpp"
    "${LIBRARY_DIR}/src/rose/rose_build_infix.cpp"
    "${LIBRARY_DIR}/src/rose/rose_build_instructions.cpp"
    "${LIBRARY_DIR}/src/rose/rose_build_lit_accel.cpp"
    "${LIBRARY_DIR}/src/rose/rose_build_long_lit.cpp"
    "${LIBRARY_DIR}/src/rose/rose_build_lookaround.cpp"
    "${LIBRARY_DIR}/src/rose/rose_build_matchers.cpp"
    "${LIBRARY_DIR}/src/rose/rose_build_merge.cpp"
    "${LIBRARY_DIR}/src/rose/rose_build_misc.cpp"
    "${LIBRARY_DIR}/src/rose/rose_build_misc.cpp"
    "${LIBRARY_DIR}/src/rose/rose_build_program.cpp"
    "${LIBRARY_DIR}/src/rose/rose_build_role_aliasing.cpp"
    "${LIBRARY_DIR}/src/rose/rose_build_scatter.cpp"
    "${LIBRARY_DIR}/src/rose/rose_build_width.cpp"
    "${LIBRARY_DIR}/src/rose/rose_in_util.cpp"
    "${LIBRARY_DIR}/src/rose/stream.c"

    "${LIBRARY_DIR}/src/smallwrite/smallwrite_build.cpp"

    "${LIBRARY_DIR}/src/som/slot_manager.cpp"
    "${LIBRARY_DIR}/src/som/som_runtime.c"
    "${LIBRARY_DIR}/src/som/som_stream.c"

    "${LIBRARY_DIR}/src/util/alloc.cpp"
    "${LIBRARY_DIR}/src/util/charreach.cpp"
    "${LIBRARY_DIR}/src/util/clique.cpp"
    "${LIBRARY_DIR}/src/util/compile_context.cpp"
    "${LIBRARY_DIR}/src/util/compile_error.cpp"
    "${LIBRARY_DIR}/src/util/depth.cpp"
    "${LIBRARY_DIR}/src/util/fatbit_build.cpp"
    "${LIBRARY_DIR}/src/util/multibit.c"
    "${LIBRARY_DIR}/src/util/multibit_build.cpp"
    "${LIBRARY_DIR}/src/util/report_manager.cpp"
    "${LIBRARY_DIR}/src/util/state_compress.c"
    "${LIBRARY_DIR}/src/util/target_info.cpp"
    "${LIBRARY_DIR}/src/util/ue2string.cpp"
)

# The original build system invokes ragel on src/parser/{Parser|control_verbs}.rl (+ a few more .rl files which are unneeded). To avoid a
# build-time dependency on ragel (via contrib/ or find_program()), add the manually generated output of ragel to the sources.
#
# Please regenerate these files if you update vectorscan. They must be regenerated for each platform separately because ragel produces for
# weird reasons different constants in the output.
#
# Also, please use upstream versions of colm and ragel, the packages in Ubuntu 22.04 seem to produce wrong output on ARM.
if (ARCH_AMD64)
    list (APPEND SRCS
        "${LIBRARY_DIR}/../vectorscan-cmake/rageled_files/amd64/Parser.cpp"
        "${LIBRARY_DIR}/../vectorscan-cmake/rageled_files/amd64/control_verbs.cpp"
    )
elseif (ARCH_AARCH64)
    list (APPEND SRCS
        "${LIBRARY_DIR}/../vectorscan-cmake/rageled_files/aarch64/Parser.cpp"
        "${LIBRARY_DIR}/../vectorscan-cmake/rageled_files/aarch64/control_verbs.cpp"
    )
    set_source_files_properties(
        "${LIBRARY_DIR}/../vectorscan-cmake/rageled_files/aarch64/Parser.cpp"
        "${LIBRARY_DIR}/../vectorscan-cmake/rageled_files/aarch64/control_verbs.cpp"
        COMPILE_FLAGS -Wno-c++11-narrowing
    )
endif()

# Platform-dependent files
if (ARCH_AMD64)
    list(APPEND SRCS
        "${LIBRARY_DIR}/src/util/arch/x86/cpuid_flags.c"
        "${LIBRARY_DIR}/src/util/arch/x86/masked_move.c"
        "${LIBRARY_DIR}/src/util/supervector/arch/x86/impl.cpp"
    )
endif()

if (ARCH_AARCH64)
    list(APPEND SRCS
        "${LIBRARY_DIR}/src/util/arch/arm/cpuid_flags.c"
        "${LIBRARY_DIR}/src/util/supervector/arch/arm/impl.cpp"
    )
endif()

add_library (_vectorscan ${SRCS})

# library has too much debug information
if (OMIT_HEAVY_DEBUG_SYMBOLS)
    target_compile_options (_vectorscan PRIVATE -g0)
endif()

target_include_directories (_vectorscan SYSTEM PUBLIC "${LIBRARY_DIR}/src")

# Makes the version header visible. It was generated by running the native build system manually.
# Please update whenever you update vectorscan.
target_include_directories (_vectorscan SYSTEM PUBLIC common)

# vectorscan inherited some patched in-source versions of boost headers to fix a bug in
# boost 1.69. This bug has been solved long ago but vectorscan's source code still
# points to the patched versions, so include it here.
target_include_directories (_vectorscan SYSTEM PRIVATE "${LIBRARY_DIR}/include")

# Include platform-specific config header generated by manually running the original build system
# Please regenerate these files if you update vectorscan.

if (ARCH_AMD64)
    target_include_directories (_vectorscan PRIVATE amd64)
endif ()

if (ARCH_AARCH64)
    target_include_directories (_vectorscan PRIVATE aarch64)
endif ()

target_link_libraries (_vectorscan PRIVATE boost::headers_only)

add_library (ch_contrib::vectorscan ALIAS _vectorscan)
