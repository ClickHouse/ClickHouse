#
#  Copyright (C) 2013-2016 MariaDB Corporation AB
#
#  Redistribution and use is allowed according to the terms of the New
#  BSD license.
#  For details see the COPYING-CMAKE-SCRIPTS file.
#
INCLUDE(CheckFunctionExists)
INCLUDE(CheckLibraryExists)
 
FUNCTION(SEARCH_LIBRARY library_name function liblist)
  IF(${${library_name}})
    RETURN()
  ENDIF()
  CHECK_FUNCTION_EXISTS(${function} ${function}_IS_SYS_FUNC)
  # check if function is part of libc
  IF(HAVE_${function}_IS_SYS_FUNC)
    SET(${library_name} "" PARENT_SCOPE)
    RETURN()
  ENDIF()
  FOREACH(lib ${liblist})
    CHECK_LIBRARY_EXISTS(${lib} ${function} "" HAVE_${function}_IN_${lib})
    IF(HAVE_${function}_IN_${lib})
      SET(${library_name} ${lib} PARENT_SCOPE)
      SET(HAVE_${library_name} 1 PARENT_SCOPE)
      RETURN()
    ENDIF()
  ENDFOREACH()
ENDFUNCTION()

