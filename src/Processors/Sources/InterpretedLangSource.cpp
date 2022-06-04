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

namespace
{

class CodeGenerator
{
public:
    CodeGenerator(std::stringstream& output_, const String& function_name_, const Strings& args_, const Strings& func_lines_) :
        output(output_),
        function_name(function_name_),
        args(args_),
        func_lines(func_lines_)
    {
        std::stringstream arg_stream;
        if (args.empty())
        {
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Functions without arguments are not supported");
        }
        arg_stream << args[0];
        for (size_t i = 1; i < args.size(); ++i) {
            arg_stream << ", " << args[i];
        }
        arg_str = arg_stream.str();
    }

    virtual void generateImports() = 0;
    virtual void generateFunction() = 0;
    virtual void generateMain() = 0;
    virtual ~CodeGenerator() = default;

protected:
    std::stringstream& output;
    const String& function_name;
    const Strings& args;
    const Strings& func_lines;
    String arg_str;
};

class PythonCodeGenerator : public CodeGenerator
{
    using CodeGenerator::CodeGenerator;

public:
    void generateImports() override
    {
        output << "import sys\n"
                  "import os\n"
                  "import numpy as np\n"
                  "import scipy\n";  /// available by default  !! config imports
    }

    void generateFunction() override
    {
        output << "def " << function_name << "(" << arg_str << "):\n";
        for (const String& line : func_lines) {
            output << "    " << line << '\n';
        }
    }

    void generateMain() override
    {
        // !! use more efficient format
        // !! make array size configurable (split input into chunks)
        output << "rows = []\n"
                  "for line in sys.stdin:\n"
                  "    rows.append([float(x) for x in line.split('\\t')])\n"  // !! convert input to array based on arg types
                  "input_data = np.array(rows)\n";

        output << "result_column = " << function_name << "(input_data[:,0]";
        for (size_t i = 1; i < args.size(); ++i) {
            output << ", input_data[:," << i << "]";
        }
        output << ")\n";

        output << "assert result_column.ndim == 1\n"
                  "for value in result_column:\n"
                  "    sys.stdout.write(str(value)+'\\n')\n"
                  "os.unlink(sys.argv[0])\n";  // FIXME doesn't work in userns
    }
};

class JuliaCodeGenerator : public CodeGenerator
{
    using CodeGenerator::CodeGenerator;

public:
    void generateImports() override
    {
    }

    void generateFunction() override
    {
        output << "function " << function_name << "(" << arg_str << ")\n";
        for (const String& line : func_lines) {
            output << "    " << line << '\n';  /// not necessary, just easier debugging
        }
        output << "end\n";
    }

    void generateMain() override
    {
        for (const String& arg : args) {
            output << arg << " = Vector{Float64}()\n";  /// init columns
        }

        output << "for line in eachline(stdin)\n"
                  "    row = parse.(Float64, split(line, '\\t'))\n";  // !! use real types
        for (size_t i = 0; i < args.size(); ++i) {
            output << "    push!(" << args[i] << ", row[" << i + 1 << "])\n";
        }
        output << "end\n";

        output << "for value in " << function_name << "(" << arg_str << ")\n"
                  "    println(value)\n"
                  "end\n"
                  "rm(PROGRAM_FILE)\n";
    }
};

class RCodeGenerator : public CodeGenerator
{
public:
    RCodeGenerator(std::stringstream& output_, const String& function_name_, const Strings& args_, const Strings& func_lines_,
                   const String& script_path_) : CodeGenerator(output_, function_name_, args_, func_lines_), script_path(script_path_)
    {
    }

    void generateImports() override
    {
    }

    void generateFunction() override
    {
        output << function_name << " <- function(" << arg_str << ") {\n";
        for (const String& line : func_lines) {
            output << "    " << line << '\n';  /// not necessary, just easier debugging
        }
        output << "}\n";
    }

    void generateMain() override
    {
        for (const String& arg : args) {
            output << arg << " <- NULL\n";  /// init columns
        }
        output << "input <- file(\"stdin\")\n"
                  "open(input, blocking=TRUE)\n"
                  "i <- 1\n";

        output << "while (length(line <- readLines(input, n=1)) > 0) {\n"
                  "    values <- as.double(unlist(strsplit(line, \"\\t\")))\n";  // !! use real types
        for (size_t arg_num = 0; arg_num < args.size(); ++arg_num) {
            /// Idk why assignment by index works (couldn't find this anywhere in docs),
            /// but it's much faster than append
            output << "    " << args[arg_num] << "[i] <- values[" << arg_num + 1 << "]\n";
        }
        output << "    i <- i + 1\n"
                  "}\n";

        output << "for (value in " << function_name << "(" << arg_str << ")) {\n"
                  "    write(value, stdout())\n"
                  "}\n"
                  "unlink(\"" << script_path << "\")\n";
    }

private:
    const String& script_path;
};

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
    Strings args;
    {
        const auto & columns = input_pipes.back().getHeader().getColumnsWithTypeAndName();
        // !! implement for functions with 0 args
        if (columns.empty())
        {
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Functions without arguments are not supported");
        }
        args.push_back(columns[0].name);
        for (size_t i = 1; i < columns.size(); ++i) {
            args.push_back(columns[i].name);
        }
    }
    std::vector<String> func_lines;
    boost::split(func_lines, function_body, [](char c) { return c == '\n'; });

    TemporaryFile script;
    script.keep();
    String command;
    std::stringstream code_str;

    /// TODO Cache generated code?
    std::optional<std::variant<PythonCodeGenerator, JuliaCodeGenerator, RCodeGenerator>> gen;
    if (configuration.interpreter_type == "python") {
        /// Test: CREATE FUNCTION foo_py(x, y) 'return x + y' USING python
        command = "/usr/bin/python3";  // !! configure
        gen.emplace(std::in_place_type<PythonCodeGenerator>, code_str, function_name, args, func_lines);
    } else if (configuration.interpreter_type == "julia") {
        /// Test: CREATE FUNCTION foo_jl(x, y) 'return x .+ y' USING julia
        command = "/usr/bin/julia";
        gen.emplace(std::in_place_type<JuliaCodeGenerator>, code_str, function_name, args, func_lines);
    } else if (configuration.interpreter_type == "R") {
        /// Test: CREATE FUNCTION foo_r(x, y) 'return(x + y)' USING R
        /// NOTE integer overflows produce NAs, and output cannot be parsed
        command = "/usr/bin/Rscript";
        gen.emplace(std::in_place_type<RCodeGenerator>, code_str, function_name, args, func_lines, script.path());
    } else {
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Unknown interpreter type: '{}'", configuration.interpreter_type);
    }
    std::visit([](CodeGenerator& codegen) {
       codegen.generateImports();  /// TODO make configurable
       codegen.generateFunction();  /// final
       codegen.generateMain();  /// TODO optimize, use real types for args and return value, split input
    }, *gen);
    String code = code_str.str();
    WriteBufferFromFile out(script.path(), code.size());
    writeString(code, out);
    out.next();
    if (context->getSettingsRef().fsync_metadata)
        out.sync();
    out.close();
    chmod(script.path().c_str(), 0644);  // FIXME temp files created by root are not readable by other users

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
