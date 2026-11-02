#!/usr/bin/env bash

# Run this script to generate header and source files in folder "./generated"
# from grammar files PromQLParser.g4, PromQLLexer.g4

# Get the directory of this script.
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Generated header and source files will be put into folder "./generated".
ANTLR4_GRAMMARS_OUTDIR="${SCRIPT_DIR}/generated/antlr4_grammars"
rm -rf "${SCRIPT_DIR}/generated"
mkdir -p "${ANTLR4_GRAMMARS_OUTDIR}"

# Create a Python virtual environment to install ANTLR4 tool.
ANTLR4_TOOLS_ENV_DIR=$(mktemp -d)
python3 -m venv "${ANTLR4_TOOLS_ENV_DIR}"

# Install the ANTLR4 tool.
"${ANTLR4_TOOLS_ENV_DIR}/bin/pip3" install antlr4-tools

# Use the ANTLR tool to generate header and source files in folder ./generated
(cd "${SCRIPT_DIR}/../antlr4-grammars/promql" && "${ANTLR4_TOOLS_ENV_DIR}/bin/antlr4" -o "${ANTLR4_GRAMMARS_OUTDIR}" -Dlanguage=Cpp -visitor PromQLParser.g4 PromQLLexer.g4 -package antlr4_grammars)

# Remove the temporary directory.
rm -rf "${ANTLR4_TOOLS_ENV_DIR}"
