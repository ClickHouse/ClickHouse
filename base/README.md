Contains general C / C++ utilities used in the project.

Per project:
* `base/`: Many small utilities, including basic type definition. They are independent and unrelated to the database logic.
* `glibc-compatibility/`: Wrappers around glibc to keep compatibility with old glibc versions. The aim for the future is to remove this and depend on a static musl build instead (see https://github.com/ClickHouse/ClickHouse/issues/75847).
* `harmful/`: A library that traps whenever harmful functions from libc are called. Used to detect dangerous libc function usage in CI.
* `pcg-random/`: External library to generate random numbers.
* `poco/`: A set of C++ libraries to build programs. This is forked and modified from the original. We aim to slowly remove it from the project completely with our own code or STD when possible, not update it or add new features to it.
* `readpassphrase/`: External library to read passwords via terminal prompt.
* `widechar_width/`: External library to implement `wcwidth` which measures the width of unicode strings.
