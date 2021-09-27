## Copyright (c) Microsoft Corporation. All rights reserved.
## SPDX-License-Identifier: MIT

##############                      TRANSPORT ADAPTER BUILD                     ######################
#  Default: If no option is explicitly added, libcurl will be used for POSIX and WinHTTP for Windows #
#  Windows: Both CURL and WINHTTP can be built to be used.                                           #
#  POSIX: Only CURL is acceptable. If WINHTTP is set, generate step will fail for user               #

if (BUILD_TRANSPORT_CUSTOM)
  message("Using the user-defined transport adapter. Make sure `AzureSdkGetCustomHttpTransport` is implemented and linked.")
  add_compile_definitions(BUILD_TRANSPORT_CUSTOM_ADAPTER)
endif()

#  Defines `BUILD_TRANSPORT_WINHTTP_ADAPTER` and `BUILD_CURL_HTTP_TRANSPORT_ADAPTER` for source code

# On Windows: Make sure to build WinHTTP either if it was user-requested or no transport was selected at all.
# On POSIX: Make sure to build Curl either if it was user-requested or no transport was selected at all.
if (WIN32 OR MINGW OR MSYS OR CYGWIN)
  if (BUILD_TRANSPORT_CURL)
    add_compile_definitions(BUILD_CURL_HTTP_TRANSPORT_ADAPTER)
  endif()
  if (BUILD_TRANSPORT_WINHTTP OR (NOT BUILD_TRANSPORT_CURL AND NOT BUILD_TRANSPORT_CUSTOM))
    message("By default, if no option is selected, on Windows, WinHTTP transport adapter is used.")
    add_compile_definitions(BUILD_TRANSPORT_WINHTTP_ADAPTER)
    if(AZ_ALL_LIBRARIES)
      SET(BUILD_TRANSPORT_WINHTTP ON)
    endif()
  endif()
elseif (UNIX)
  if (BUILD_TRANSPORT_WINHTTP)
    message(FATAL_ERROR "WinHTTP transport adapter is not supported for POSIX platforms.")
  endif()
  if (BUILD_TRANSPORT_CURL OR (NOT BUILD_TRANSPORT_CUSTOM))
    message("By default, if no option is selected, on POSIX, libcurl transport adapter is used.")
    add_compile_definitions(BUILD_CURL_HTTP_TRANSPORT_ADAPTER)
    if(AZ_ALL_LIBRARIES)
      SET(BUILD_TRANSPORT_CURL ON)
    endif()
  endif()
else()
  message(FATAL_ERROR "Unsupported platform.")
endif()
