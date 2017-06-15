#
#  Copyright (C) 2013-2016 MariaDB Corporation AB
#
#  Redistribution and use is allowed according to the terms of the New
#  BSD license.
#  For details see the COPYING-CMAKE-SCRIPTS file.
#
MACRO(create_symlink symlink_name target install_path)
# According to cmake documentation symlinks work on unix systems only
IF(UNIX)
  # Get target components 
  GET_TARGET_PROPERTY(target_location ${target} LOCATION)
  GET_FILENAME_COMPONENT(target_path ${target_location} PATH)
  GET_FILENAME_COMPONENT(target_name ${target_location} NAME)

  ADD_CUSTOM_COMMAND(
    OUTPUT ${target_path}/${symlink_name}
    COMMAND ${CMAKE_COMMAND} ARGS -E remove -f ${target_path}/${symlink_name}
    COMMAND ${CMAKE_COMMAND} ARGS -E create_symlink ${target_name} ${symlink_name}
    WORKING_DIRECTORY ${target_path}
    DEPENDS ${target}
    )
  
  ADD_CUSTOM_TARGET(SYM_${symlink_name}
    ALL
    DEPENDS ${target_path}/${symlink_name})
  SET_TARGET_PROPERTIES(SYM_${symlink_name} PROPERTIES CLEAN_DIRECT_OUTPUT 1)

  IF(CMAKE_GENERATOR MATCHES "Xcode")
    # For Xcode, replace project config with install config
    STRING(REPLACE "${CMAKE_CFG_INTDIR}" 
      "\${CMAKE_INSTALL_CONFIG_NAME}" output ${target_path}/${symlink_name})
  ENDIF()

  # presumably this will be used for libmysql*.so symlinks
  INSTALL(FILES ${target_path}/${symlink_name} DESTINATION ${install_path}
          COMPONENT SharedLibraries)
ENDIF()
ENDMACRO()
