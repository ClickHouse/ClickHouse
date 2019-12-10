# File containing various utilities

# Returns a list of arguments that evaluate to true
function(count_true output_count_var)
  set(lst)
  foreach(option_var IN LISTS ARGN)
    if(${option_var})
      list(APPEND lst ${option_var})
    endif()
  endforeach()
  list(LENGTH lst lst_len)
  set(${output_count_var} ${lst_len} PARENT_SCOPE)
endfunction()
