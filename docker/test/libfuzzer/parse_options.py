#!/usr/bin/env python3
# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
################################################################################
"""Helper script for parsing custom fuzzing options."""
import configparser
import sys


def parse_options(options_file_path, options_section):
    """Parses the given file and returns options from the given section."""
    parser = configparser.ConfigParser()
    parser.read(options_file_path)

    if not parser.has_section(options_section):
        return None

    options = parser[options_section]

    if options_section == "libfuzzer":
        options_string = " ".join(
            "-%s=%s" % (key, value) for key, value in options.items()
        )
    else:
        # Sanitizer options.
        options_string = ":".join(
            "%s=%s" % (key, value) for key, value in options.items()
        )

    return options_string


def main():
    """Processes the arguments and prints the options in the correct format."""
    if len(sys.argv) < 3:
        sys.stderr.write(
            "Usage: %s <path_to_options_file> <options_section>\n" % sys.argv[0]
        )
        return 1

    options = parse_options(sys.argv[1], sys.argv[2])
    if options is not None:
        print(options)

    return 0


if __name__ == "__main__":
    sys.exit(main())
