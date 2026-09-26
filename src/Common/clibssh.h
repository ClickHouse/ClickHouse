#pragma once
/*
Include this file to access libssh api.
*/

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wreserved-macro-identifier"
#pragma GCC diagnostic ignored "-Wreserved-identifier"
#pragma GCC diagnostic ignored "-Wdocumentation"
#include <libssh/libssh.h> // IWYU pragma: export
#include <libssh/server.h> // IWYU pragma: export
#include <libssh/callbacks.h> // IWYU pragma: export
#pragma GCC diagnostic pop
