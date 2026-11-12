// Protocol Buffers - Google's data interchange format
// Copyright 2008 Google Inc.  All rights reserved.
//
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file or at
// https://developers.google.com/open-source/licenses/bsd

// protoc's compiler/main.cc from contrib/google-protobuf, registering only the C++ generator.

#include "absl/log/initialize.h"
#include "google/protobuf/compiler/command_line_interface.h"
#include "google/protobuf/compiler/cpp/generator.h"

int main(int argc, char * argv[])
{
    absl::InitializeLog();
    google::protobuf::compiler::CommandLineInterface cli;
    cli.AllowPlugins("protoc-");

    google::protobuf::compiler::cpp::CppGenerator cpp_generator;
    cli.RegisterGenerator("--cpp_out", "--cpp_opt", &cpp_generator, "Generate C++ header and source.");

    return cli.Run(argc, argv);
}
