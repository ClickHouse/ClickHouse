#pragma once
/*
Include this file to access libssh api.
*/

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wreserved-macro-identifier"
#pragma clang diagnostic ignored "-Wreserved-identifier"
#pragma clang diagnostic ignored "-Wdocumentation"
#include <libssh/libssh.h> // IWYU pragma: export
#include <libssh/server.h> // IWYU pragma: export
#include <libssh/callbacks.h> // IWYU pragma: export
#pragma clang diagnostic pop
