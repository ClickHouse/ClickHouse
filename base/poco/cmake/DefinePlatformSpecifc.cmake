# http://www.cmake.org/Wiki/CMake_Useful_Variables :
# CMAKE_BUILD_TYPE
#    Choose the type of build. CMake has default flags for these:
#
#    * None (CMAKE_C_FLAGS or CMAKE_CXX_FLAGS used)
#    * Debug (CMAKE_C_FLAGS_DEBUG or CMAKE_CXX_FLAGS_DEBUG)
#    * Release (CMAKE_C_FLAGS_RELEASE or CMAKE_CXX_FLAGS_RELEASE)
#    * RelWithDebInfo (CMAKE_C_FLAGS_RELWITHDEBINFO or CMAKE_CXX_FLAGS_RELWITHDEBINFO
#    * MinSizeRel (CMAKE_C_FLAGS_MINSIZEREL or CMAKE_CXX_FLAGS_MINSIZEREL)

# Setting CXX Flag /MD or /MT and POSTFIX values i.e MDd / MD / MTd / MT / d
#
# For visual studio the library naming is as following:
#  Dynamic libraries:
#   - PocoX.dll  for release library
#   - PocoXd.dll for debug library
#
#  Static libraries:
#   - PocoXmd.lib for /MD release build
#   - PocoXtmt.lib for /MT release build
#
#   - PocoXmdd.lib for /MD debug build
#   - PocoXmtd.lib for /MT debug build

if(MSVC)
    if(POCO_MT)
        set(CompilerFlags
            CMAKE_CXX_FLAGS
            CMAKE_CXX_FLAGS_DEBUG
            CMAKE_CXX_FLAGS_RELEASE
            CMAKE_C_FLAGS
            CMAKE_C_FLAGS_DEBUG
            CMAKE_C_FLAGS_RELEASE
            )
        foreach(CompilerFlag ${CompilerFlags})
            string(REPLACE "/MD" "/MT" ${CompilerFlag} "${${CompilerFlag}}")
        endforeach()

        set(STATIC_POSTFIX "mt" CACHE STRING "Set static library postfix" FORCE)
    else(POCO_MT)
        set(STATIC_POSTFIX "md" CACHE STRING "Set static library postfix" FORCE)
    endif(POCO_MT)
      
    if (ENABLE_MSVC_MP)
      add_definitions(/MP)
    endif()
    
else(MSVC)
    # Other compilers then MSVC don't have a static STATIC_POSTFIX at the moment
    set(STATIC_POSTFIX "" CACHE STRING "Set static library postfix" FORCE)
    set(CMAKE_C_FLAGS_DEBUG   "${CMAKE_C_FLAGS_DEBUG}   -D_DEBUG")
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -D_DEBUG")
endif(MSVC)



# Add a d postfix to the debug libraries
if(POCO_STATIC)
        set(CMAKE_DEBUG_POSTFIX "${STATIC_POSTFIX}d" CACHE STRING "Set Debug library postfix" FORCE)
        set(CMAKE_RELEASE_POSTFIX "${STATIC_POSTFIX}" CACHE STRING "Set Release library postfix" FORCE)
        set(CMAKE_MINSIZEREL_POSTFIX "${STATIC_POSTFIX}" CACHE STRING "Set MinSizeRel library postfix" FORCE)
        set(CMAKE_RELWITHDEBINFO_POSTFIX "${STATIC_POSTFIX}" CACHE STRING "Set RelWithDebInfo library postfix" FORCE)
else(POCO_STATIC)
        set(CMAKE_DEBUG_POSTFIX "d" CACHE STRING "Set Debug library postfix" FORCE)
        set(CMAKE_RELEASE_POSTFIX "" CACHE STRING "Set Release library postfix" FORCE)
        set(CMAKE_MINSIZEREL_POSTFIX "" CACHE STRING "Set MinSizeRel library postfix" FORCE)
        set(CMAKE_RELWITHDEBINFO_POSTFIX "" CACHE STRING "Set RelWithDebInfo library postfix" FORCE)
endif()


# OS Detection
include(CheckTypeSize)
find_package(Cygwin)

if(WIN32)
  add_definitions( -DPOCO_OS_FAMILY_WINDOWS -DUNICODE -D_UNICODE -D__LCC__)  #__LCC__ define used by MySQL.h
endif(WIN32)

if (CYGWIN)
  add_definitions(-DPOCO_NO_FPENVIRONMENT -DPOCO_NO_WSTRING)
  add_definitions(-D_XOPEN_SOURCE=500 -D__BSD_VISIBLE)
else (CYGWIN)
	if (UNIX AND NOT ANDROID )
	  add_definitions( -DPOCO_OS_FAMILY_UNIX )
	  # Standard 'must be' defines
	  if (APPLE)
	    add_definitions( -DPOCO_HAVE_IPv6 -DPOCO_NO_STAT64)
	    set(SYSLIBS  ${CMAKE_DL_LIBS})
	  else (APPLE)
	    add_definitions( -D_REENTRANT -D_THREAD_SAFE -D_LARGEFILE64_SOURCE -D_FILE_OFFSET_BITS=64 )
	    if (QNX)
	      add_definitions( -DPOCO_HAVE_FD_POLL)
	      set(SYSLIBS  m socket)
		elseif(CMAKE_SYSTEM MATCHES "FreeBSD")
			add_definitions(-D_XOPEN_SOURCE=600 -D__BSD_VISIBLE -D_REENTRANT -D_THREAD_SAFE -D_LARGEFILE64_SOURCE -D_FILE_OFFSET_BITS=64 -DPOCO_HAVE_IPv6 -DPOCO_HAVE_FD_POLL)
			set(SYSLIBS pthread ${CMAKE_DL_LIBS} rt)
	    else ()
	      add_definitions( -D_XOPEN_SOURCE=500)
	      if (${CMAKE_SYSTEM} MATCHES "AIX")
	        add_definitions( -DPOCO_HAVE_FD_POLL)
	      else()
	        add_definitions( -DPOCO_HAVE_FD_EPOLL)
	      endif()
	      set(SYSLIBS  pthread ${CMAKE_DL_LIBS} rt)
	    endif ()
	  endif (APPLE)
	endif(UNIX AND NOT ANDROID )
endif (CYGWIN)

if (CMAKE_SYSTEM MATCHES "SunOS")
  add_definitions( -DPOCO_OS_FAMILY_UNIX )
  # Standard 'must be' defines
  add_definitions( -D_XOPEN_SOURCE=500 -D_REENTRANT -D_THREAD_SAFE -D_LARGEFILE64_SOURCE -D_FILE_OFFSET_BITS=64 )
  set(SYSLIBS  pthread socket xnet nsl resolv rt ${CMAKE_DL_LIBS})
endif(CMAKE_SYSTEM MATCHES "SunOS")

if (CMAKE_COMPILER_IS_MINGW)
  add_definitions(-DWC_NO_BEST_FIT_CHARS=0x400  -DPOCO_WIN32_UTF8)
  add_definitions(-D_WIN32 -DMINGW32 -DWINVER=0x500 -DODBCVER=0x0300 -DPOCO_THREAD_STACK_SIZE)
endif (CMAKE_COMPILER_IS_MINGW)

# SunPro C++
if (${CMAKE_CXX_COMPILER_ID} MATCHES "SunPro")
  add_definitions( -D_BSD_SOURCE -library=stlport4)
endif (${CMAKE_CXX_COMPILER_ID} MATCHES "SunPro")

# iOS
if (IOS)
  add_definitions( -DPOCO_HAVE_IPv6 -DPOCO_NO_FPENVIRONMENT -DPOCO_NO_STAT64 -DPOCO_NO_SHAREDLIBS -DPOCO_NO_NET_IFTYPES )
endif(IOS)

#Android
if (ANDROID)
  add_definitions( -DPOCO_NO_FPENVIRONMENT -DPOCO_NO_WSTRING -DPOCO_NO_SHAREDMEMORY )
endif(ANDROID)
