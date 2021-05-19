---
toc_priority: 61
toc_title: Working with code coverage
---

# ClickHouse Coverage Report {#clickhouse-coverage-report}

ClickHouse Coverage Report (referred as CCR) is a simple format for accumulating large coverage reports while 
preserving per-test coverage data. It can be easily converted to `.info` for `genhtml`. ClickHouse includes a CCR 
converter located at `/docker/test/coverage/ccr_converter.py`.

CCR consists of 3 sections:

Header:

```
/absolute/path/to/ch/src/directory
FILES <source files count>
<source file 1 relative path from src/> <functions> <lines>
<sf 1 function 1 mangled name> <function start line> <function edge index>
<sf 1 function 2 mangled name> <function start line> <function edge index>
<sf 1 instrumented line 1>
<sf 1 instrumented line 2>
<source file 2 relative path from src/> <functions> <lines>
```

Test entry:

```
TEST
SOURCE <source file id> <functions count> <lines count>
<function 1 edge index> <call count>
<function 2 edge index> <call count>
<line 1 number> <call count>
<line 2 number> <call count>
```

Footer:

```
TESTS
<test 1 name>
<test 2 name>
```
