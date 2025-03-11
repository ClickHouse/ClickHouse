# Intel® QuickAssist Technology ZSTD Plugin (QAT ZSTD Plugin) is a plugin to Zstandard*(ZSTD*) for accelerating compression by QAT.
# ENABLE_QAT_OUT_OF_TREE_BUILD = 1 means kernel don't have native support, user will build and install driver from external package: https://www.intel.com/content/www/us/en/download/765501.html
# meanwhile, user need to set ICP_ROOT environment variable which point to the root directory of QAT driver source tree.
# ENABLE_QAT_OUT_OF_TREE_BUILD = 0 means kernel has built-in qat driver, QAT-ZSTD-PLUGIN just has dependency on qatlib.

if (ENABLE_QAT_OUT_OF_TREE_BUILD)
    message(STATUS "Intel QATZSTD out-of-tree build, ICP_ROOT:${ICP_ROOT}")

    set(QATZSTD_SRC_DIR "${ClickHouse_SOURCE_DIR}/contrib/QAT-ZSTD-Plugin/src")
    set(QATZSTD_SRC "${QATZSTD_SRC_DIR}/qatseqprod.c")
    set(ZSTD_LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/zstd/lib")
    set(QAT_INCLUDE_DIR "${ICP_ROOT}/quickassist/include")
    set(QAT_DC_INCLUDE_DIR "${ICP_ROOT}/quickassist/include/dc")
    set(QAT_AL_INCLUDE_DIR "${ICP_ROOT}/quickassist/lookaside/access_layer/include")
    set(QAT_USDM_INCLUDE_DIR "${ICP_ROOT}/quickassist/utilities/libusdm_drv")
    set(USDM_LIBRARY "${ICP_ROOT}/build/libusdm_drv_s.so")
    set(QAT_S_LIBRARY "${ICP_ROOT}/build/libqat_s.so")
    if (ENABLE_QAT_USDM_DRIVER)
        add_definitions(-DENABLE_USDM_DRV)
    endif()
    add_library(_qatzstd_plugin ${QATZSTD_SRC})
    target_link_libraries (_qatzstd_plugin PUBLIC  ${USDM_LIBRARY} ${QAT_S_LIBRARY})
    target_include_directories(_qatzstd_plugin
        SYSTEM PUBLIC "${QATZSTD_SRC_DIR}"
        PRIVATE ${QAT_INCLUDE_DIR}
                ${QAT_DC_INCLUDE_DIR}
                ${QAT_AL_INCLUDE_DIR}
                ${QAT_USDM_INCLUDE_DIR}
                ${ZSTD_LIBRARY_DIR})
    target_compile_definitions(_qatzstd_plugin PRIVATE -DDEBUGLEVEL=0)
    add_library (ch_contrib::qatzstd_plugin ALIAS _qatzstd_plugin)
else () # In-tree build
    message(STATUS "Intel QATZSTD in-tree build")
    set(QATZSTD_SRC_DIR "${ClickHouse_SOURCE_DIR}/contrib/QAT-ZSTD-Plugin/src")
    set(QATZSTD_SRC "${QATZSTD_SRC_DIR}/qatseqprod.c")
    set(ZSTD_LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/zstd/lib")

    # please download&build ICP package from: https://www.intel.com/content/www/us/en/download/765501.html
    set(ICP_ROOT "${ClickHouse_SOURCE_DIR}/contrib/qatlib")
    set(QAT_INCLUDE_DIR "${ICP_ROOT}/quickassist/include")
    set(QAT_DC_INCLUDE_DIR "${ICP_ROOT}/quickassist/include/dc")
    set(QAT_AL_INCLUDE_DIR "${ICP_ROOT}/quickassist/lookaside/access_layer/include")
    set(QAT_USDM_INCLUDE_DIR "${ICP_ROOT}/quickassist/utilities/libusdm_drv")
    set(USDM_LIBRARY "${ICP_ROOT}/build/libusdm_drv_s.so")
    set(QAT_S_LIBRARY "${ICP_ROOT}/build/libqat_s.so")
    set(LIBQAT_ROOT_DIR "${ClickHouse_SOURCE_DIR}/contrib/qatlib")
    set(LIBQAT_HEADER_DIR "${CMAKE_CURRENT_BINARY_DIR}/include")

    file(MAKE_DIRECTORY
        "${LIBQAT_HEADER_DIR}/qat"
    )
    file(COPY "${LIBQAT_ROOT_DIR}/quickassist/include/cpa.h"
        DESTINATION "${LIBQAT_HEADER_DIR}/qat/"
    )
    file(COPY "${LIBQAT_ROOT_DIR}/quickassist/include/dc/cpa_dc.h"
        DESTINATION "${LIBQAT_HEADER_DIR}/qat/"
    )
    file(COPY "${LIBQAT_ROOT_DIR}/quickassist/lookaside/access_layer/include/icp_sal_poll.h"
        DESTINATION "${LIBQAT_HEADER_DIR}/qat/"
    )
    file(COPY "${LIBQAT_ROOT_DIR}/quickassist/lookaside/access_layer/include/icp_sal_user.h"
        DESTINATION "${LIBQAT_HEADER_DIR}/qat/"
    )
    file(COPY "${LIBQAT_ROOT_DIR}/quickassist/utilities/libusdm_drv/qae_mem.h"
        DESTINATION "${LIBQAT_HEADER_DIR}/qat/"
    )

    if (ENABLE_QAT_USDM_DRIVER)
        add_definitions(-DENABLE_USDM_DRV)
    endif()

    add_library(_qatzstd_plugin ${QATZSTD_SRC})
    target_link_libraries (_qatzstd_plugin PUBLIC ch_contrib::qatlib ch_contrib::usdm)
    target_include_directories(_qatzstd_plugin PRIVATE
        ${QAT_INCLUDE_DIR}
        ${QAT_DC_INCLUDE_DIR}
        ${QAT_AL_INCLUDE_DIR}
        ${QAT_USDM_INCLUDE_DIR}
        ${ZSTD_LIBRARY_DIR}
        ${LIBQAT_HEADER_DIR})
    target_compile_definitions(_qatzstd_plugin PRIVATE -DDEBUGLEVEL=0 PUBLIC -DINTREE)
    target_include_directories(_qatzstd_plugin SYSTEM PUBLIC $<BUILD_INTERFACE:${QATZSTD_SRC_DIR}> $<INSTALL_INTERFACE:include>)
    add_library (ch_contrib::qatzstd_plugin ALIAS _qatzstd_plugin)
endif ()

