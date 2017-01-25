# В этом файле описаны действия для добавления init.d скрипта. Пока в разработке.
# Данный файл нужно включать в CMakeLists.txt в каталоге каждого конкретного демона.
# Пример использования:
# include (${ClickHouse_SOURCE_DIR}/create_init_script.cmake)
# create_init_script (divider Divider)
# Будет создан init.d скрипт с названием divider для демона (бинарника) Divider

macro (create_init_script daemonname)
	set (filename ${daemonname})
	# Опционально принимаем filename вторым аргументом.
	set (extra_args ${ARGN})
	list (LENGTH extra_args num_extra_args)
	if (${num_extra_args} GREATER 0)
		list (GET extra_args 0 optional_arg)
		set (filename ${optional_arg})
	endif ()
	set (tmp_file_name ${filename}.init)

	set (SED_INPLACE_SUFFIX "")
	if (APPLE OR CMAKE_SYSTEM MATCHES "FreeBSD")
		set (SED_INPLACE_SUFFIX "''")
	endif ()
	add_custom_command (OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/${tmp_file_name}
			COMMAND sed -e 's,[@]DAEMON[@],${daemonname},g' < ${PROJECT_SOURCE_DIR}/tools/init.d/template > ${CMAKE_CURRENT_BINARY_DIR}/${tmp_file_name}
			COMMAND sed -i ${SED_INPLACE_SUFFIX} 's,[@]CRONFILE[@],${filename},g' ${CMAKE_CURRENT_BINARY_DIR}/${tmp_file_name}
			COMMAND chmod a+x ${CMAKE_CURRENT_BINARY_DIR}/${tmp_file_name}
			COMMENT "Building ${daemonname}"
			)
	add_custom_target (${daemonname}-init.target DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/${tmp_file_name})
	install (
			FILES ${CMAKE_CURRENT_BINARY_DIR}/${tmp_file_name}
			DESTINATION /etc/init.d
			RENAME ${filename}
			PERMISSIONS OWNER_EXECUTE OWNER_READ GROUP_EXECUTE GROUP_READ WORLD_EXECUTE WORLD_READ
			COMPONENT ${daemonname}
			)
	add_dependencies (${daemonname} ${daemonname}-init.target)
endmacro (create_init_script)
