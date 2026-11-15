#!/usr/bin/env python3

import string
import sys

TOKEN_TEXT = 1
TOKEN_VAR = 2

TOKEN_COLON = ":"
TOKEN_SEMI = ";"
TOKEN_OR = "|"
TOKEN_QUESTIONMARK = "?"
TOKEN_ROUND_BRACKET_OPEN = "("
TOKEN_ROUND_BRACKET_CLOSE = ")"
TOKEN_ASTERISK = "*"
TOKEN_SLASH = "/"


class TextValue:
    def __init__(self, t):
        self.t = t
        self.slug = None

    def get_slug(self):
        if self.slug is not None:
            return self.slug
        slug = ""
        for c in self.t:
            slug += c if c in string.ascii_letters else "_"
        self.slug = slug
        return slug

    def get_name(self):
        return f"TextValue_{self.get_slug()}"

    def __repr__(self):
        return f'TextValue("{self.t}")'


class Var:
    def __init__(self, id_):
        self.id_ = id_

    def __repr__(self):
        return f"Var({self.id_})"


class Parser:
    def __init__(self):
        self.chains = []
        self.text = None
        self.col = 0
        self.line = 1
        self.t = None
        self.var_id = -1
        self.cur_tok = None
        self.includes = []

        self.proto = ""
        self.cpp = ""

    def parse_file(self, filename):
        with open(filename) as f:
            self.text = f.read()

        while self.parse_statement() is not None:
            pass

    def add_include(self, filename):
        self.includes.append(filename)

    def get_next_token(self):
        self.skip_ws()

        if not len(self.text):
            return None

        if self.text[0] == '"':
            return self.parse_txt_value()

        if self.text[0] == "$":
            return self.parse_var_value()

        c, self.text = self.text[0], self.text[1:]
        self.cur_tok = c
        return c

    def parse_var_value(self):
        i = self.text.find(" ")

        id_, self.text = self.text[1:i], self.text[i + 1 :]
        self.var_id = int(id_)
        self.cur_tok = TOKEN_VAR
        return TOKEN_VAR

    def parse_txt_value(self):
        if self.text[0] != '"':
            raise Exception("parse_txt_value: expected quote at the start")

        self.t = ""
        self.text = self.text[1:]

        while self.text[0] != '"':
            if self.text[0] == "\\":
                if self.text[1] == "x":
                    self.t += self.text[:4]
                    self.text = self.text[4:]
                elif self.text[1] in 'nt\\"':
                    self.t += self.text[:2]
                    self.text = self.text[2:]
                else:
                    raise Exception(f"parse_txt_value: unknown symbol {self.text[0]}")
            else:
                c, self.text = self.text[0], self.text[1:]
                self.t += c

        self.text = self.text[1:]
        self.cur_tok = TOKEN_TEXT
        return TOKEN_TEXT

    def skip_ws(self):
        while self.text and self.text[0] in string.whitespace:
            if self.text[0] == "\n":
                self.line += 1
                self.col = 0
            self.text = self.text[1:]
            self.col += 1
        if not self.text:
            return None
        return True

    def skip_line(self):
        self.line += 1
        index = self.text.find("\n")
        self.text = self.text[index:]

    def parse_statement(self):
        if self.skip_ws() is None:
            return None

        self.get_next_token()
        if self.cur_tok == TOKEN_SLASH:
            self.skip_line()
            return TOKEN_SLASH

        chain = []
        while self.cur_tok != TOKEN_SEMI:
            if self.cur_tok == TOKEN_TEXT:
                chain.append(TextValue(self.t))
            elif self.cur_tok == TOKEN_VAR:
                chain.append(Var(self.var_id))
            else:
                self.fatal_parsing_error(f"unexpected token {self.cur_tok}")
            self.get_next_token()

        if not chain:
            self.fatal_parsing_error("empty chains are not allowed")
        self.chains.append(chain)
        return True

    def generate(self):
        self.proto = 'syntax = "proto3";\n\n'
        self.cpp = "#include <iostream>\n#include <string>\n#include <vector>\n\n#include <libfuzzer/libfuzzer_macro.h>\n\n"

        for incl_file in self.includes:
            self.cpp += f'#include "{incl_file}"\n'
        self.cpp += "\n"

        self.proto += "message Word {\n"
        self.proto += "\tenum Value {\n"

        self.cpp += "void GenerateWord(const Word&, std::string&, int);\n\n"

        self.cpp += (
            "void GenerateSentence(const Sentence& stc, std::string &s, int depth) {\n"
        )
        self.cpp += "\tfor (int i = 0; i < stc.words_size(); i++ ) {\n"
        self.cpp += "\t\tGenerateWord(stc.words(i), s, ++depth);\n"
        self.cpp += "\t}\n"
        self.cpp += "}\n"

        self.cpp += "void GenerateWord(const Word& word, std::string &s, int depth) {\n"

        self.cpp += "\tif (depth > 5) return;\n\n"
        self.cpp += "\tswitch (word.value()) {\n"

        for idx, chain in enumerate(self.chains):
            self.proto += f"\t\tvalue_{idx} = {idx};\n"

            self.cpp += f"\t\tcase {idx}: {{\n"
            num_var = 0
            for item in chain:
                if isinstance(item, TextValue):
                    self.cpp += f'\t\t\ts += "{item.t}";\n'
                elif isinstance(item, Var):
                    self.cpp += f"\t\t\tif (word.inner().words_size() > {num_var})\t\t\t\tGenerateWord(word.inner().words({num_var}), s, ++depth);\n"
                    num_var += 1
                else:
                    raise Exception("unknown token met during generation")
            self.cpp += "\t\t\tbreak;\n\t\t}\n"
        self.cpp += "\t\tdefault: break;\n"

        self.cpp += "\t}\n"

        self.proto += "\t}\n"
        self.proto += "\tValue value = 1;\n"
        self.proto += "\tSentence inner = 2;\n"
        self.proto += "}\nmessage Sentence {\n\trepeated Word words = 1;\n}"

        self.cpp += "}\n"
        return self.cpp, self.proto

    def fatal_parsing_error(self, msg):
        print(f"Line: {self.line}, Col: {self.col}")
        raise Exception(f"fatal error during parsing. {msg}")


def main(args):
    input_file, outfile_cpp, outfile_proto = args

    if not outfile_proto.endswith(".proto"):
        raise Exception("outfile_proto (argv[3]) should end with `.proto`")

    include_filename = outfile_proto[:-6] + ".pb.h"

    p = Parser()
    p.add_include(include_filename)
    p.parse_file(input_file)

    cpp, proto = p.generate()

    proto = proto.replace("\t", " " * 4)
    cpp = cpp.replace("\t", " " * 4)

    with open(outfile_cpp, "w") as f:
        f.write(cpp)

    with open(outfile_proto, "w") as f:
        f.write(proto)


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print(f"Usage {sys.argv[0]} <input_file> <outfile.cpp> <outfile.proto>")
        sys.exit(1)
    main(sys.argv[1:])
