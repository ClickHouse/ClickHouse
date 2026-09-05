set (LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/replxx")

set(SRCS
    "${LIBRARY_DIR}/src/conversion.cxx"
    "${LIBRARY_DIR}/src/ConvertUTF.cpp"
    "${LIBRARY_DIR}/src/escape.cxx"
    "${LIBRARY_DIR}/src/history.cxx"
    "${LIBRARY_DIR}/src/terminal.cxx"
    "${LIBRARY_DIR}/src/prompt.cxx"
    "${LIBRARY_DIR}/src/replxx_impl.cxx"
    "${LIBRARY_DIR}/src/replxx.cxx"
    "${LIBRARY_DIR}/src/util.cxx"
    "${LIBRARY_DIR}/src/wcwidth.cpp"
)

if (OS_WINDOWS)
    # The Win32 console backend: `win_write`, which translates the ANSI escapes replxx emits
    # into `SetConsoleTextAttribute` calls, and the saved console attributes it needs.
    list (APPEND SRCS "${LIBRARY_DIR}/src/windows.cxx")
    # Defines the `replxx::tty::out` that `windows.cxx` needs and the fork dropped; see the file.
    list (APPEND SRCS "${CMAKE_CURRENT_SOURCE_DIR}/replxx-windows-tty.cxx")
endif ()

add_library (_replxx ${SRCS})
target_include_directories(_replxx SYSTEM PUBLIC "${LIBRARY_DIR}/include")

if (OS_WINDOWS)
    # See replxx-windows-compat.h: supplies `dprintf`/`fsync` for the descriptor writes that
    # ClickHouse's fork added without a Windows branch.
    target_compile_options(_replxx PRIVATE
        "SHELL:-include ${CMAKE_CURRENT_SOURCE_DIR}/replxx-windows-compat.h")
endif ()

add_library(ch_contrib::replxx ALIAS _replxx)
