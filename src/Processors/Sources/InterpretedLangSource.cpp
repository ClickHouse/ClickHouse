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

Pipe InterpretedLangSourceCoordinator::createPipe(const String& function_body, std::vector<Pipe> && input_pipes, Block sample_block, ContextPtr context) {
    String command, code, args;
    size_t arg_count;
    {
        const auto & columns = input_pipes.back().getHeader().getColumnsWithTypeAndName();
        arg_count = columns.size();
        std::stringstream arg_str;
        // !! implement for functions with 0 args
        if (columns.empty())
        {
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Functions without arguments are not supported");
        }
        arg_str << columns[0].name;
        for (size_t i = 1; i < columns.size(); ++i) {
            arg_str << ", " << columns[i].name;
        }
        args = arg_str.str();
    }

    if (configuration.interpreter_type == "python") {
        command = "/usr/bin/python3";  // !! configure
        std::stringstream code_str;
        code_str << "import sys\n";
        code_str << "import os\n";
        code_str << "import numpy as np\n";
        code_str << "def udf(" << args << "):\n";
        std::vector<String> func_lines;
        boost::split(func_lines, function_body, [](char c) { return c == '\n'; });
        for (const String& line : func_lines) {
            code_str << "    " << line << '\n';
        }
        // !! use more efficient format
        // !! convert input to array based on arg types
        // !! make array size configurable (split input into chunks)
        code_str << "rows = []\n";
        code_str << "for line in sys.stdin:\n";
        code_str << "    rows.append([int(x) for x in line.split('\\t')])\n";
        code_str << "input_data = np.array(rows)\n";
        code_str << "result_column = udf(input_data[:,0]";
        for (size_t i = 1; i < arg_count; ++i) {
            code_str << ", input_data[:," << i << "]";
        }
        code_str << ")\n";
        code_str << "assert result_column.ndim == 1\n";
        code_str << "for value in result_column:\n";
        code_str << "    sys.stdout.write(str(value)+'\\n')\n";
        code_str << "# os.unlink(sys.argv[0])\n";
        code = code_str.str();
    } else if (configuration.interpreter_type == "julia") {
        command = "julia";
        code = "";  // !! add impl
    } else if (configuration.interpreter_type == "R") {
        command = "R";
        code = "";  // !! add impl
    } else {
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Unknown interpreter type: '{}'", configuration.interpreter_type);
    }
    TemporaryFile script;
    script.keep();
    WriteBufferFromFile out(script.path(), code.size());
    writeString(code, out);
    out.next();
    if (context->getSettingsRef().fsync_metadata)
        out.sync();
    out.close();

    // !! remove script / use memfd?
    return cmd_coordinator.createPipe(
        command,
        {script.path()},
        std::move(input_pipes),
        sample_block,
        context,
        {});
}

}
