# - Try to find metrohash headers and libraries.
#
# Usage of this module as follows:
#
#     find_package(metrohash)
#
# Variables used by this module, they can change the default behaviour and need
# to be set before calling find_package:
#
#  METROHASH_ROOT_DIR Set this variable to the root installation of
#                    metrohash if the module has problems finding
#                    the proper installation path.
#
# Variables defined by this module:
#
#  METROHASH_FOUND             System has metrohash libs/headers
#  METROHASH_LIBRARIES         The metrohash library/libraries
#  METROHASH_INCLUDE_DIR       The location of metrohash headers

find_path(METROHASH_ROOT_DIR
    NAMES include/metrohash.h
)

find_library(METROHASH_LIBRARIES
    NAMES metrohash
    PATHS ${METROHASH_ROOT_DIR}/lib ${METROHASH_LIBRARIES_PATHS}
)

find_path(METROHASH_INCLUDE_DIR
    NAMES metrohash.h
    PATHS ${METROHASH_ROOT_DIR}/include ${METROHASH_INCLUDE_PATHS}
)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(metrohash DEFAULT_MSG
    METROHASH_LIBRARIES
    METROHASH_INCLUDE_DIR
)

mark_as_advanced(
    METROHASH_ROOT_DIR
    METROHASH_LIBRARIES
    METROHASH_INCLUDE_DIR
)
