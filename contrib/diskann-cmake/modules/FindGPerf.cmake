# - Try to find Google performance tools (gperftools)
# Input variables:
#   GPERFTOOLS_ROOT_DIR    - The gperftools install directory;
#                            if not set the GPERFTOOLS_DIR environment variable will be used
#   GPERFTOOLS_INCLUDE_DIR - The gperftools include directory
#   GPERFTOOLS_LIBRARY     - The gperftools library directory
# Components: profiler, and tcmalloc or tcmalloc_minimal
# Output variables:
#   GPERFTOOLS_FOUND        - System has gperftools
#   GPERFTOOLS_INCLUDE_DIRS - The gperftools include directories
#   GPERFTOOLS_LIBRARIES    - The libraries needed to use gperftools
#   GPERFTOOLS_VERSION      - The version string for gperftools
include(FindPackageHandleStandardArgs)
  
if(NOT DEFINED GPERFTOOLS_FOUND)

  # If not set already, set GPERFTOOLS_ROOT_DIR from environment
  if (DEFINED ENV{GPERFTOOLS_DIR} AND NOT DEFINED GPERFTOOLS_ROOT_DIR)
    set(GPERFTOOLS_ROOT_DIR $ENV{GPERFTOOLS_DIR})
  endif()

  # Check to see if libunwind is required
  set(GPERFTOOLS_DISABLE_PROFILER FALSE)
  if((";${Gperftools_FIND_COMPONENTS};" MATCHES ";profiler;") AND 
      (CMAKE_SYSTEM_NAME MATCHES "Linux" OR 
       CMAKE_SYSTEM_NAME MATCHES "BlueGeneQ" OR
       CMAKE_SYSTEM_NAME MATCHES "BlueGeneP") AND
       (CMAKE_SIZEOF_VOID_P EQUAL 8))
       
    # Libunwind is required by profiler on this platform
    if(Gperftools_FIND_REQUIRED_profiler OR Gperftools_FIND_REQUIRED_tcmalloc_and_profiler)
      find_package(Libunwind 0.99 REQUIRED)
    else()
      find_package(Libunwind)
      if(NOT LIBUNWIND_FOUND OR LIBUNWIND_VERSION VERSION_LESS 0.99)
        set(GPERFTOOLS_DISABLE_PROFILER TRUE)
      endif()
    endif()
  endif()

  # Check for invalid components
  foreach(_comp ${Gperftools_FIND_COMPONENTS})
    if((NOT _comp STREQUAL "tcmalloc_and_profiler") AND
       (NOT _comp STREQUAL "tcmalloc") AND
       (NOT _comp STREQUAL "tcmalloc_minimal") AND
       (NOT _comp STREQUAL "profiler"))
      message(FATAL_ERROR "Invalid component specified for Gperftools: ${_comp}")
    endif()
  endforeach()

  # Check for valid component combinations
  if(";${Gperftools_FIND_COMPONENTS};" MATCHES ";tcmalloc_and_profiler;" AND 
      (";${Gperftools_FIND_COMPONENTS};" MATCHES ";tcmalloc;" OR 
       ";${Gperftools_FIND_COMPONENTS};" MATCHES ";tcmalloc_minimal;" OR
       ";${Gperftools_FIND_COMPONENTS};" MATCHES ";profiler;"))
    message("ERROR: Invalid component selection for Gperftools: ${Gperftools_FIND_COMPONENTS}")
    message("ERROR: Gperftools cannot link both tcmalloc_and_profiler with the tcmalloc, tcmalloc_minimal, or profiler libraries")
    message(FATAL_ERROR "Gperftools component list is invalid")
  endif()
  if(";${Gperftools_FIND_COMPONENTS};" MATCHES ";tcmalloc;" AND ";${Gperftools_FIND_COMPONENTS};" MATCHES ";tcmalloc_minimal;")
    message("ERROR: Invalid component selection for Gperftools: ${Gperftools_FIND_COMPONENTS}")
    message("ERROR: Gperftools cannot link both tcmalloc and tcmalloc_minimal")
    message(FATAL_ERROR "Gperftools component list is invalid")
  endif()

  # Set default sarch paths for gperftools
  if(GPERFTOOLS_ROOT_DIR)
    set(GPERFTOOLS_INCLUDE_DIR ${GPERFTOOLS_ROOT_DIR}/include CACHE PATH "The include directory for gperftools")
    if(CMAKE_SIZEOF_VOID_P EQUAL 8 AND CMAKE_SYSTEM_NAME STREQUAL "Linux")
      set(GPERFTOOLS_LIBRARY ${GPERFTOOLS_ROOT_DIR}/lib64;${GPERFTOOLS_ROOT_DIR}/lib CACHE PATH "The library directory for gperftools")
    else()
      set(GPERFTOOLS_LIBRARY ${GPERFTOOLS_ROOT_DIR}/lib CACHE PATH "The library directory for gperftools")
    endif()
  endif()
  
  find_path(GPERFTOOLS_INCLUDE_DIRS NAMES gperftools/malloc_extension.h
      HINTS ${GPERFTOOLS_INCLUDE_DIR})

  # Search for component libraries
  foreach(_comp ${Gperftools_FIND_COMPONENTS})
    find_library(GPERFTOOLS_${_comp}_LIBRARY ${_comp} 
        HINTS ${GPERFTOOLS_LIBRARY})
    if(GPERFTOOLS_${_comp}_LIBRARY)
      set(Gperftools_${_comp}_FOUND TRUE)
    else()
      set(Gperftools_${_comp}_FOUND FALSE)
    endif()
    
    # Exclude profiler from the found list if libunwind is required but not found
    if(Gperftools_${_comp}_FOUND AND ${_comp} MATCHES "profiler" AND GPERFTOOLS_DISABLE_PROFILER)
      set(Gperftools_${_comp}_FOUND FALSE)
      set(GPERFTOOLS_${_comp}_LIBRARY "GPERFTOOLS_${_comp}_LIBRARY-NOTFOUND")
      message("WARNING: Gperftools '${_comp}' requires libunwind 0.99 or later.")
      message("WARNING: Gperftools '${_comp}' will be disabled.")
    endif()
    
    if(";${Gperftools_FIND_COMPONENTS};" MATCHES ";${_comp};" AND Gperftools_${_comp}_FOUND)
      list(APPEND GPERFTOOLS_LIBRARIES "${GPERFTOOLS_${_comp}_LIBRARY}")
    endif()
  endforeach()
  
  # Set gperftools libraries if not set based on component list
  if(NOT GPERFTOOLS_LIBRARIES)
    if(Gperftools_tcmalloc_and_profiler_FOUND)
      set(GPERFTOOLS_LIBRARIES "${GPERFTOOLS_tcmalloc_and_profiler_LIBRARY}")
    elseif(Gperftools_tcmalloc_FOUND AND GPERFTOOLS_profiler_FOUND)
      set(GPERFTOOLS_LIBRARIES "${GPERFTOOLS_tcmalloc_LIBRARY}" "${GPERFTOOLS_profiler_LIBRARY}")
    elseif(Gperftools_profiler_FOUND)
      set(GPERFTOOLS_LIBRARIES "${GPERFTOOLS_profiler_LIBRARY}")
    elseif(Gperftools_tcmalloc_FOUND)
      set(GPERFTOOLS_LIBRARIES "${GPERFTOOLS_tcmalloc_LIBRARY}")
    elseif(Gperftools_tcmalloc_minimal_FOUND)
      set(GPERFTOOLS_LIBRARIES "${GPERFTOOLS_tcmalloc_minimal_LIBRARY}")
    endif()
  endif()

  # handle the QUIETLY and REQUIRED arguments and set GPERFTOOLS_FOUND to TRUE
  # if all listed variables are TRUE
  find_package_handle_standard_args(Gperftools
      FOUND_VAR GPERFTOOLS_FOUND
      REQUIRED_VARS GPERFTOOLS_LIBRARIES GPERFTOOLS_INCLUDE_DIRS
      HANDLE_COMPONENTS)

  mark_as_advanced(GPERFTOOLS_INCLUDE_DIR GPERFTOOLS_LIBRARY 
      GPERFTOOLS_INCLUDE_DIRS GPERFTOOLS_LIBRARIES)

  # Add linker flags that instruct the compiler to exclude built in memory
  # allocation functions. This works for GNU, Intel, and Clang. Other compilers
  # may need to be added in the future.
  if(GPERFTOOLS_LIBRARIES MATCHES "tcmalloc")
    if((CMAKE_CXX_COMPILER_ID MATCHES "GNU") OR
       (CMAKE_CXX_COMPILER_ID MATCHES "AppleClang") OR 
       (CMAKE_CXX_COMPILER_ID MATCHES "Clang") OR
      ((CMAKE_CXX_COMPILER_ID MATCHES "Intel") AND (NOT CMAKE_CXX_PLATFORM_ID MATCHES "Windows"))) 
      list(APPEND GPERFTOOLS_LIBRARIES "-fno-builtin-malloc"
          "-fno-builtin-calloc" "-fno-builtin-realloc" "-fno-builtin-free")
    endif()
  endif()

  # Add libunwind flags to gperftools if the profiler is being used
  if(GPERFTOOLS_LIBRARIES MATCHES "profiler" AND LIBUNWIND_FOUND)
    list(APPEND GPERFTOOLS_INCLUDE_DIRS "${LIBUNWIND_INCLUDE_DIR}")
    list(APPEND GPERFTOOLS_LIBRARIES "${LIBUNWIND_LIBRARIES}")
  endif()
  
  unset(GPERFTOOLS_DISABLE_PROFILER)

endif()
