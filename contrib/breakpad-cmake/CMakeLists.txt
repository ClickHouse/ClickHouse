if (NOT OS_LINUX)
    if (ENABLE_BREAKPAD)
        message (${RECONFIGURE_MESSAGE_LEVEL}
                 "breakpad is disabled implicitly, it only support linux now")
    endif ()
    set (ENABLE_BREAKPAD OFF)
elseif (NOT ARCH_AMD64)
    if (ENABLE_BREAKPAD)
        message (${RECONFIGURE_MESSAGE_LEVEL}
                 "breakpad is disabled implicitly, it only support amd64 now")
    endif ()
    set (ENABLE_BREAKPAD OFF)
else()
    option (ENABLE_BREAKPAD "Enable breakpad minidump" ${ENABLE_LIBRARIES})
endif ()

if (NOT ENABLE_BREAKPAD)
    message (STATUS "Not using breakpad:" )
    return()
endif ()

set(BREAKPAD_DIR "${ClickHouse_SOURCE_DIR}/contrib/breakpad/src")

SET(BREAKPAD_SRC_FILES
        # for lib_breakpad.a
        "${BREAKPAD_DIR}/client/linux/crash_generation/crash_generation_client.cc"
        "${BREAKPAD_DIR}/client/linux/crash_generation/crash_generation_server.cc"
        "${BREAKPAD_DIR}/client/linux/dump_writer_common/thread_info.cc"
        "${BREAKPAD_DIR}/client/linux/dump_writer_common/ucontext_reader.cc"
        "${BREAKPAD_DIR}/client/linux/handler/exception_handler.cc"
        "${BREAKPAD_DIR}/client/linux/handler/minidump_descriptor.cc"
        "${BREAKPAD_DIR}/client/linux/log/log.cc"
        "${BREAKPAD_DIR}/client/linux/microdump_writer/microdump_writer.cc"
        "${BREAKPAD_DIR}/client/linux/minidump_writer/linux_core_dumper.cc"
        "${BREAKPAD_DIR}/client/linux/minidump_writer/linux_dumper.cc"
        "${BREAKPAD_DIR}/client/linux/minidump_writer/linux_ptrace_dumper.cc"
        "${BREAKPAD_DIR}/client/linux/minidump_writer/minidump_writer.cc"
        "${BREAKPAD_DIR}/client/linux/minidump_writer/pe_file.cc"
        "${BREAKPAD_DIR}/client/minidump_file_writer.cc"
        "${BREAKPAD_DIR}/common/convert_UTF.cc"
        "${BREAKPAD_DIR}/common/md5.cc"
        "${BREAKPAD_DIR}/common/string_conversion.cc"
        "${BREAKPAD_DIR}/common/linux/elf_core_dump.cc"
        "${BREAKPAD_DIR}/common/linux/elfutils.cc"
        "${BREAKPAD_DIR}/common/linux/file_id.cc"
        "${BREAKPAD_DIR}/common/linux/guid_creator.cc"
        "${BREAKPAD_DIR}/common/linux/linux_libc_support.cc"
        "${BREAKPAD_DIR}/common/linux/memory_mapped_file.cc"
        "${BREAKPAD_DIR}/common/linux/safe_readlink.cc"
        "${BREAKPAD_DIR}/common/linux/breakpad_getcontext.S"
        # for Minidump2Core
        "${BREAKPAD_DIR}/common/linux/memory_mapped_file.cc"
        "${BREAKPAD_DIR}/common/path_helper.cc"
        "Minidump2Core.cpp"
        "ExceptionHandler.cpp"
)

add_library(_breakpad ${BREAKPAD_SRC_FILES})
add_library(ch_contrib::breakpad ALIAS _breakpad)
target_include_directories(_breakpad SYSTEM BEFORE PUBLIC "${BREAKPAD_DIR}")
