option (USE_INTERNAL_CLD2_LIBRARY "Use internal cld2 library" ${NOT_UNBUNDLED})

if (NOT USE_INTERNAL_LZ4_LIBRARY)
    find_library (LIBRARY_CLD2 cld2)
    find_path (INCLUDE_CLD2 compact_lang_det.h)

    if (LIBRARY_CLD2 AND INCLUDE_CLD2)
        set(EXTERNAL_CLD2_LIBRARY_FOUND 1)
        add_library (cld2 INTERFACE)
        set_property (TARGET cld2 PROPERTY INTERFACE_LINK_LIBRARIES ${LIBRARY_CLD2})
        set_property (TARGET cld2 PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${INCLUDE_CLD2})
    else ()
        set(EXTERNAL_CLD2_LIBRARY_FOUND 0)
        message (${RECONFIGURE_MESSAGE_LEVEL} "Can't find system cld2")
    endif()
endif()

if (NOT EXTERNAL_CLD2_LIBRARY_FOUND)
    set (USE_INTERNAL_CLD2_LIBRARY 1)
    set (LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/cld2")

    set (SRCS
        ${LIBRARY_DIR}/internal/cldutil.cc
        ${LIBRARY_DIR}/internal/cldutil_shared.cc
        ${LIBRARY_DIR}/internal/compact_lang_det.cc
        ${LIBRARY_DIR}/internal/compact_lang_det_hint_code.cc
        ${LIBRARY_DIR}/internal/compact_lang_det_impl.cc
        ${LIBRARY_DIR}/internal/debug.cc
        ${LIBRARY_DIR}/internal/fixunicodevalue.cc
        ${LIBRARY_DIR}/internal/generated_entities.cc
        ${LIBRARY_DIR}/internal/generated_language.cc
        ${LIBRARY_DIR}/internal/generated_ulscript.cc
        ${LIBRARY_DIR}/internal/getonescriptspan.cc
        ${LIBRARY_DIR}/internal/lang_script.cc
        ${LIBRARY_DIR}/internal/offsetmap.cc
        ${LIBRARY_DIR}/internal/scoreonescriptspan.cc
        ${LIBRARY_DIR}/internal/tote.cc
        ${LIBRARY_DIR}/internal/utf8statetable.cc
        ${LIBRARY_DIR}/internal/cld_generated_cjk_uni_prop_80.cc
        ${LIBRARY_DIR}/internal/cld2_generated_cjk_compatible.cc
        ${LIBRARY_DIR}/internal/cld_generated_cjk_delta_bi_4.cc
        ${LIBRARY_DIR}/internal/generated_distinct_bi_0.cc
        ${LIBRARY_DIR}/internal/cld2_generated_quadchrome_2.cc
        ${LIBRARY_DIR}/internal/cld2_generated_deltaoctachrome.cc
        ${LIBRARY_DIR}/internal/cld2_generated_distinctoctachrome.cc
        ${LIBRARY_DIR}/internal/cld_generated_score_quad_octa_2.cc
    )

    add_library(cld2 ${SRCS})

    target_compile_options (cld2
        PRIVATE
            -Wno-old-style-cast
            -Wno-inconsistent-missing-destructor-override
            -Wno-deprecated
            -Wno-unused-parameter
            -Wno-shadow
            -Wno-tautological-type-limit-compare
            -Wno-extra-semi
            -Wno-narrowing
            -Wl
    )

    target_include_directories (cld2 SYSTEM PUBLIC ${LIBRARY_DIR}/public)
endif()
    #target_link_libraries (cld2 PUBLIC ssl)

