#!/usr/bin/env bash

# Run this script to generate header and source files in folder ./generated
# from grammar files PromQLParser.g4, PromQLLexer.g4

# Get the directory of this script.
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Create a Python virtual environment to install ANTLR4 tool.
ANTLR4_TOOLS_ENV_DIR="${SCRIPT_DIR}/_antlr4-tools-env"
if [ ! -f "${ANTLR4_TOOLS_ENV_DIR}/bin/python3" ]; then
    python3 -m venv "${ANTLR4_TOOLS_ENV_DIR}"
fi

# Install the ANTLR4 tool.
if [ ! -f "${ANTLR4_TOOLS_ENV_DIR}/bin/antlr4" ]; then
    "${ANTLR4_TOOLS_ENV_DIR}/bin/pip3" install antlr4-tools
fi

# Use the ANTLR tool to generate header and source files in folder ./generated
rm -rf "${SCRIPT_DIR}/generated"
mkdir "${SCRIPT_DIR}/generated"

(cd "${SCRIPT_DIR}/../antlr4-grammars/promql" && "${ANTLR4_TOOLS_ENV_DIR}/bin/antlr4" -o "${SCRIPT_DIR}/generated" -Dlanguage=Cpp -visitor PromQLParser.g4 PromQLLexer.g4)
