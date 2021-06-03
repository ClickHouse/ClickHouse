CMAKE_MINIMUM_REQUIRED(VERSION 2.6)

PROJECT(wnb)

# Boost dependency
#--------------------------------------------------

# IF (${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
#   SET (BOOST_ROOT /Developer/boost_build/) # Suggested path
# ELSE()
#   SET (BOOST_ROOT "/usr/include")
# ENDIF()
##############
SET (BOOST_ROOT "${ClickHouse_SOURCE_DIR}/contrib/boost")
##############
MESSAGE(STATUS "** Search Boost root: ${BOOST_ROOT}")
FIND_PACKAGE(Boost 1.70.0 COMPONENTS graph REQUIRED)
MESSAGE(STATUS "** Boost Include: ${Boost_INCLUDE_DIR}")
MESSAGE(STATUS "** Boost Libraries: ${Boost_LIBRARY_DIRS}")
MESSAGE(STATUS "** Boost Libraries: ${Boost_LIBRARIES}")

INCLUDE_DIRECTORIES(${Boost_INCLUDE_DIR})
LINK_DIRECTORIES(${Boost_LIBRARY_DIRS})

# Project
#--------------------------------------------------

LINK_DIRECTORIES(${wnb_SOURCE_DIR}/lib)
INCLUDE_DIRECTORIES(${PROJECT_BINARY_DIR})

SET(PROJECT_VERSION "0.6")
SET(ARCHIVE_NAME ${CMAKE_PROJECT_NAME}-${PROJECT_VERSION})

ADD_CUSTOM_TARGET(dist
  COMMAND git archive --prefix=${ARCHIVE_NAME}/ HEAD
  | bzip2 > ${CMAKE_BINARY_DIR}/${ARCHIVE_NAME}.tar.bz2
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})

ADD_CUSTOM_TARGET(check
  COMMAND ./check/check.sh ./check/list.txt)


## Compiler flags
IF (CMAKE_COMPILER_IS_GNUCXX)
  list(APPEND CMAKE_CXX_FLAGS " --std=c++11 -O3 -DNDEBUG -Wall -Wextra")
  #list(APPEND CMAKE_CXX_FLAGS " -g -Wall -Wextra")
ENDIF()

SET(WNB_SRCS wnb/core/wordnet.cc
  wnb/core/load_wordnet.cc wnb/core/info_helper.cc)

# Executable
#--------------------------------------------------
ADD_EXECUTABLE (wntest wnb/main.cc ${WNB_SRCS})
SET(EXECUTABLE_OUTPUT_PATH ${wnb_BINARY_DIR}/bin)

# Static library
#--------------------------------------------------
ADD_LIBRARY(wnb ${WNB_SRCS})
SET(LIBRARY_OUTPUT_PATH ${wnb_BINARY_DIR}/lib)

IF (Boost_FOUND)
  TARGET_LINK_LIBRARIES(wntest ${Boost_LIBRARIES})
  TARGET_LINK_LIBRARIES(wnb ${Boost_LIBRARIES})
ENDIF()