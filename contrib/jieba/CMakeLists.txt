add_library(_jieba jieba.cpp jieba_dict.cpp jieba_mp_seg.cpp jieba_hmm_seg.cpp)
target_include_directories(_jieba SYSTEM PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(_jieba PUBLIC ch_contrib::abseil_swiss_tables ch_contrib::darts-clone ch_contrib::incbin)
add_library(ch_contrib::jieba ALIAS _jieba)
