#include <boost/algorithm/string.hpp>

#include <Processors/Sources/InterpretedLangSource.h>
#include <Common/filesystemHelpers.h>
#include <IO/WriteHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
}

InterpretedLangSourceCoordinator::InterpretedLangSourceCoordinator(const Configuration & configuration_)
    : configuration(configuration_), cmd_coordinator({ .format = "TabSeparated" })
{
}

Pipe InterpretedLangSourceCoordinator::createPipe(const String& function_name,
                                                  const String& function_body,
                                                  std::vector<Pipe> && input_pipes,
                                                  Block sample_block,
                                                  ContextPtr context)
{
    String command, code, arg_str;
    Strings args;
    {
        const auto & columns = input_pipes.back().getHeader().getColumnsWithTypeAndName();
        std::stringstream arg_stream;
        // !! implement for functions with 0 arg_str
        if (columns.empty())
        {
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Functions without arguments are not supported");
        }
        arg_stream << columns[0].name;
        args.push_back(columns[0].name);
        for (size_t i = 1; i < columns.size(); ++i) {
            arg_stream << ", " << columns[i].name;
            args.push_back(columns[i].name);
        }
        arg_str = arg_stream.str();
    }

    TemporaryFile script;
    script.keep();

    /// TODO Cache generated code?
    if (configuration.interpreter_type == "python") {
        /// Test: CREATE FUNCTION foo_py(x, y) 'return x + y' USING python
        command = "/usr/bin/python3";  // !! configure
        std::stringstream code_str;

        code_str << "import sys\n"
                    "import os\n"
                    "import numpy as np\n"
                    "import scipy\n";  /// available by default  !! config imports

        code_str << "def " << function_name << "(" << arg_str << "):\n";
        std::vector<String> func_lines;
        boost::split(func_lines, function_body, [](char c) { return c == '\n'; });
        for (const String& line : func_lines) {
            code_str << "    " << line << '\n';
        }

        // !! use more efficient format
        // !! make array size configurable (split input into chunks)
        code_str << "rows = []\n"
                    "for line in sys.stdin:\n"
                    "    rows.append([int(x) for x in line.split('\\t')])\n"  // !! convert input to array based on arg types
                    "input_data = np.array(rows)\n";

        code_str << "result_column = " << function_name << "(input_data[:,0]";
        for (size_t i = 1; i < args.size(); ++i) {
            code_str << ", input_data[:," << i << "]";
        }
        code_str << ")\n";

        code_str << "assert result_column.ndim == 1\n"
                    "for value in result_column:\n"
                    "    sys.stdout.write(str(value)+'\\n')\n"
                    "os.unlink(sys.argv[0])\n";
        code = code_str.str();
    } else if (configuration.interpreter_type == "julia") {
        /// Test: CREATE FUNCTION foo_jl(x, y) 'return x .+ y' USING julia
        command = "/usr/bin/julia";
        std::stringstream code_str;

        code_str << "function " << function_name << "(" << arg_str << ")\n";
        std::vector<String> func_lines;
        boost::split(func_lines, function_body, [](char c) { return c == '\n'; });
        for (const String& line : func_lines) {
            code_str << "    " << line << '\n';  /// not necessary, just easier debugging
        }
        code_str << "end\n";

        for (const String& arg : args) {
            code_str << arg << " = Vector{Int}()\n";
        }

        code_str << "for line in eachline(stdin)\n"
                    "    row = parse.(Int, split(line, '\\t'))\n";  // !! use real types
        for (size_t i = 0; i < args.size(); ++i) {
            code_str << "    push!(" << args[i] << ", row[" << i + 1 << "])\n";
        }
        code_str << "end\n";

        code_str << "for value in " << function_name << "(" << arg_str << ")\n"
                    "    println(value)\n"
                    "end\n"
                    "rm(PROGRAM_FILE)\n";
        code = code_str.str();
    } else if (configuration.interpreter_type == "R") {
        /// Test: CREATE FUNCTION foo_r(x, y) 'return(x + y)' USING R
        /// NOTE integer overflows produce NAs, and output cannot be parsed
        command = "/usr/bin/Rscript";
        std::stringstream code_str;

        code_str << function_name << " <- function(" << arg_str << ") {\n";
        std::vector<String> func_lines;
        boost::split(func_lines, function_body, [](char c) { return c == '\n'; });
        for (const String& line : func_lines) {
            code_str << "    " << line << '\n';  /// not necessary, just easier debugging
        }
        code_str << "}\n";

        for (const String& arg : args) {
            code_str << arg << " <- NULL\n";  /// init columns
        }
        code_str << "input <- file(\"stdin\")\n"
                    "open(input, blocking=TRUE)\n"
                    "i <- 1\n";

        code_str << "while (length(line <- readLines(input, n=1)) > 0) {\n"
                    "    values <- strtoi(unlist(strsplit(line, \"\\t\")))\n";  // !! use real types
        for (size_t arg_num = 0; arg_num < args.size(); ++arg_num) {
            /// Idk why assignment by index works (couldn't find this anywhere in docs),
            /// but it's much faster than append
            code_str << "    " << args[arg_num] << "[i] <- values[" << arg_num + 1 << "]\n";
        }
        code_str << "    i <- i + 1\n"
                    "}\n";

        code_str << "for (value in " << function_name << "(" << arg_str << ")) {\n"
                "    write(value, stdout())\n"
                "}\n"
                "unlink(\"" << script.path() << "\")\n";
        code = code_str.str();

    } else {
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Unknown interpreter type: '{}'", configuration.interpreter_type);
    }
    WriteBufferFromFile out(script.path(), code.size());
    writeString(code, out);
    out.next();
    if (context->getSettingsRef().fsync_metadata)
        out.sync();
    out.close();

    // !! remove script from outside / use memfd?
    return cmd_coordinator.createPipe(
        command,
        {script.path()},
        std::move(input_pipes),
        sample_block,
        context,
        {});
}

}
