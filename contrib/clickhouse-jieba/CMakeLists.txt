add_library(_jieba jieba_hmm_seg.cpp)
target_include_directories(_jieba SYSTEM PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
add_library(ch_contrib::jieba ALIAS _jieba)
