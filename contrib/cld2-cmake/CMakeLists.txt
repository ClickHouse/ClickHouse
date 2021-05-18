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

add_library(cld2 SHARED ${SRCS})
target_compile_options (cld2 PRIVATE -Wl -Wno-narrowing)


target_link_libraries (cld2 PRIVATE ${LIBRARY_DIR}/internal/libcld2.so)
target_include_directories(cld2 PUBLIC "${LIBRARY_DIR}/public")
