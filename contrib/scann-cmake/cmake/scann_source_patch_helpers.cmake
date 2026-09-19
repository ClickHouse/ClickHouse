# Require every source patch to match the expected upstream text exactly.
function (scann_checked_replace old_text new_text output_variable input_text)
    set(expected_count 1)
    if (ARGC GREATER 4)
        set(expected_count "${ARGV4}")
    endif ()

    string(LENGTH "${old_text}" old_text_length)
    if (old_text_length EQUAL 0)
        message(FATAL_ERROR "ScaNN patch anchor for ${output_variable} must not be empty")
    endif ()

    set(remaining_text "${input_text}")
    set(actual_count 0)
    while (TRUE)
        string(FIND "${remaining_text}" "${old_text}" match_offset)
        if (match_offset EQUAL -1)
            break ()
        endif ()

        math(EXPR actual_count "${actual_count} + 1")
        math(EXPR remaining_offset "${match_offset} + ${old_text_length}")
        string(SUBSTRING "${remaining_text}" "${remaining_offset}" -1 remaining_text)
    endwhile ()

    if (NOT actual_count EQUAL expected_count)
        message(FATAL_ERROR
            "ScaNN patch anchor for ${output_variable} matched ${actual_count} times; expected ${expected_count}")
    endif ()

    string(REPLACE "${old_text}" "${new_text}" replaced_text "${input_text}")
    set("${output_variable}" "${replaced_text}" PARENT_SCOPE)
endfunction ()
