SET(re2_headers
./re2/tostring.cc
./re2/dfa.cc
./re2/prefilter.cc
./re2/compile.cc
./re2/regexp.cc
./re2/onepass.cc
./re2/prefilter_tree.cc
./re2/set.cc
./re2/filtered_re2.cc
./re2/perl_groups.cc
./re2/parse.cc
./re2/nfa.cc
./re2/bitstate.cc
./re2/simplify.cc
./re2/unicode_groups.cc
./re2/mimics_pcre.cc
./re2/re2.cc
./re2/prog.cc
./re2/unicode_casefold.cc
./util/test.cc
./util/strutil.cc
./util/stringpiece.cc
./util/hash.cc
./util/arena.cc
./util/benchmark.cc
./util/valgrind.cc
./util/pcre.cc
./util/stringprintf.cc
./util/rune.cc
./util/random.cc
./util/thread.cc
)

# Смысл в том, чтобы собрать две версии библиотеки - потокобезопасную (re2) и непотокобезопасную (re2_st).
# Библиотека re2, при выполнении регекспа, изменяет некоторое состояние - создаёт временные DFA.
# Для того, чтобы один объект-регексп можно было использовать одновременно из разных потоков, она использует RWLock.
# При этом, даже если использовать в разных потоках разные объекты re2 (созданные из одинакового регекспа),
#  то, не смотря на отсутствие блокировок, RWLock "вхолостую" всё-равно очень существенно тормозит.
# Решение: собрать непотокобезопасную версию библиотеки и использовать разные объекты re2 в разных потоках.

add_definitions( -DNDEBUG )

add_library (re2 ${re2_headers})
add_library (re2_st ${re2_headers})

set_target_properties(re2_st PROPERTIES COMPILE_DEFINITIONS "NO_THREADS;re2=re2_st")

ADD_CUSTOM_COMMAND(
    TARGET re2_st
    POST_BUILD
    COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/create_st_headers.sh
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    COMMENT "Creating headers for re2_st library.")
