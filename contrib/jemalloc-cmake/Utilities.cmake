# Utilities.cmake
# Supporting functions to build Jemalloc

########################################################################
# CheckTypeSize
function(UtilCheckTypeSize type OUTPUT_VAR_NAME)

CHECK_TYPE_SIZE(${type} ${OUTPUT_VAR_NAME} LANGUAGE C)

if(${${OUTPUT_VAR_NAME}})
  message (STATUS "${type} size is ${${OUTPUT_VAR_NAME}}")
  set(${OUTPUT_VAR_NAME} ${${OUTPUT_VAR_NAME}} PARENT_SCOPE)
else()
  message(FATAL_ERROR "Can not determine ${type} size")
endif()

endfunction(UtilCheckTypeSize)

########################################################################
# Power of two
# returns result in a VAR whose name is in RESULT_NAME
function (pow2 e RESULT_NAME)
  set(pow2_result 1)
  while ( ${e} GREATER 0 )
    math(EXPR pow2_result "${pow2_result} + ${pow2_result}")
    math(EXPR e "${e} - 1")
  endwhile(${e} GREATER 0 )
  set(${RESULT_NAME} ${pow2_result} PARENT_SCOPE)
endfunction(pow2)

#########################################################################
# Logarithm base 2
# returns result in a VAR whose name is in RESULT_NAME
function (lg x RESULT_NAME)
  set(lg_result 0)
  while ( ${x} GREATER 1 )
    math(EXPR lg_result "${lg_result} + 1")
    math(EXPR x "${x} / 2")
  endwhile ( ${x} GREATER 1 )
  set(${RESULT_NAME} ${lg_result} PARENT_SCOPE)
endfunction(lg)

######################################################
# Based on size_class() in size_classes.sh
#
# Thankfully, no floating point calcs
#
# Defined upon return:
# - psz ("yes" or "no")
# - bin ("yes" or "no")
# - lg_delta_lookup (${lg_delta} or "no")
function(size_class index lg_grp lg_delta ndelta lg_p lg_g lg_kmax output_file)

  if(${lg_delta} GREATER ${lg_p})
    set(psz "yes")
  else()
    pow2(${lg_p} "p")
    pow2(${lg_grp} "grp")
    pow2(${lg_delta} "delta")

    math(EXPR sz "${grp} + ${delta} * ${ndelta}")
    math(EXPR npgs "${sz} / ${p}")

    # Check if the integer division above had a rem
    math(EXPR product "${npgs} * ${p}")
    if(${sz} EQUAL ${product})
      set(psz "yes")
    else()
      set(psz "no")
    endif()
  endif()

  lg( ${ndelta} "lg_ndelta")
  pow2(${lg_ndelta} "pow2_result")

  if( ${pow2_result} LESS ${ndelta})
    set(rem "yes")
  else()
    set(rem "no")
  endif()

  set(lg_size ${lg_grp})

  math(EXPR lg_delta_plus_ndelta "${lg_delta} + ${lg_ndelta}")
  if( ${lg_delta_plus_ndelta} EQUAL ${lg_grp})
    math(EXPR lg_size "${lg_grp} + 1")
  else()
    set(lg_size ${lg_grp})
    set(rem "yes")
  endif()

  # lg_g explicitely added to the list of args
  math(EXPR lg_g_plus_p "${lg_p} + ${lg_g}")
  if(${lg_size} LESS ${lg_g_plus_p})
    set(bin "yes")
  else()
    set(bin "no")
  endif()

  # AND has a higher precedence in the original SH than OR so we
  # explicitly group them together here
  if( (${lg_size} LESS ${lg_kmax}) OR
      ((${lg_size} EQUAL ${lg_kmax}) AND ("${rem}" STREQUAL "no")))
    set(lg_delta_lookup ${lg_delta})
  else()
    set(lg_delta_lookup "no")
  endif()

  ## TODO: Formatted output maybe necessary
  file (APPEND "${output_file}"
    "    SC(  ${index}, ${lg_grp},  ${lg_delta},  ${ndelta}, ${psz}, ${bin}, ${lg_delta_lookup}) \\\n"
    )

  # Defined upon return:
  # - psz ("yes" or "no")
  # - bin ("yes" or "no")
  # - lg_delta_lookup (${lg_delta} or "no")

  # Promote to PARENT_SCOPE
  set(psz ${psz} PARENT_SCOPE)
  set(bin ${bin} PARENT_SCOPE)
  set(lg_delta_lookup ${lg_delta_lookup} PARENT_SCOPE)

  # message(STATUS "size_class_result: psz: ${psz} bin: ${bin} lg_delta_lookup: ${lg_delta_lookup}")
endfunction(size_class)

####################################################################
# size_classes helper function
# Based on size_classes.sh
#
# Defined upon completion:
# - ntbins
# - nlbins
# - nbins
# - nsizes
# - npsizes
# - lg_tiny_maxclass
# - lookup_maxclass
# - small_maxclass
# - lg_large_minclass
# - huge_maxclass
function(size_classes lg_z lg_q lg_t lg_p lg_g output_file)

  math(EXPR lg_z_plus_3 "${lg_z} + 3")
  pow2 (${lg_z_plus_3} "ptr_bits")
  pow2 (${lg_g} "g")

  file(APPEND "${output_file}"
    "#define	SIZE_CLASSES \\\n"
    "  /* index, lg_grp, lg_delta, ndelta, psz, bin, lg_delta_lookup */ \\\n"
  )

  set(ntbins 0)
  set(nlbins 0)
  set(lg_tiny_maxclass "\"NA\"")
  set(nbins 0)
  set(npsizes 0)

  # Tiny size classes.
  set(ndelta 0)
  set(index 0)
  set(lg_grp ${lg_t})
  set(lg_delta ${lg_grp})

  while(${lg_grp} LESS ${lg_q})
    # Add passing lg_g as penaltimate arg. lg_g originally passed implicitly
    # See doc for the output values
    size_class(${index} ${lg_grp} ${lg_delta} ${ndelta} ${lg_p}
      ${lg_g} ${lg_kmax} "${output_file}")

    if(NOT "${lg_delta_lookup}" STREQUAL "no")
      math(EXPR nlbins "${index} + 1")
    endif()
    if(${psz} STREQUAL "yes")
      math(EXPR npsizes "${npsizes} + 1")
    endif()
    if(NOT "${bin}" STREQUAL "no")
      math(EXPR nbins "${index} + 1")
    endif()
    math(EXPR ntbins "${ntbins} + 1")

    set(lg_tiny_maxclass ${lg_grp}) # Final written value is correct.
    math(EXPR index "${index} + 1")
    set(lg_delta ${lg_grp})
    math(EXPR lg_grp "${lg_grp} + 1")
  endwhile(${lg_grp} LESS ${lg_q})

  # First non-tiny group.
  if( ${ntbins} GREATER 0)
    file(APPEND "${output_file}"
      "                                               \\\n"
    )
    # The first size class has an unusual encoding, because the size has to be
    # split between grp and delta*ndelta.
    math(EXPR lg_grp "${lg_grp} - 1")
    set (ndelta 1)
    size_class(${index} ${lg_grp} ${lg_delta} ${ndelta} ${lg_p} ${lg_g} ${lg_kmax} "${output_file}")
    math(EXPR index "${index} + 1")
    math(EXPR lg_grp "${lg_grp} + 1")
    math(EXPR lg_delta "${lg_delta} + 1")
    if(${psz} STREQUAL "yes")
      math(EXPR npsizes "${npsizes} + 1")
    endif()
  endif()

  while (${ndelta} LESS ${g})
    size_class( ${index} ${lg_grp} ${lg_delta} ${ndelta} ${lg_p} ${lg_g} ${lg_kmax} "${output_file}")
    math(EXPR index "${index} + 1")
    math(EXPR ndelta "${ndelta} + 1")
    if(${psz} STREQUAL "yes")
      math(EXPR npsizes "${npsizes} + 1")
    endif()
  endwhile (${ndelta} LESS ${g})

  # All remaining groups.
  math(EXPR lg_grp "${lg_grp} + ${lg_g}")
  math(EXPR ptr_bits_min1 "${ptr_bits} - 1")
  math(EXPR ptr_bits_min2 "${ptr_bits} - 2")
  while(${lg_grp} LESS ${ptr_bits_min1})

    file(APPEND "${output_file}"
    "                                         \\\n"
    )
    set(ndelta 1)

    if(${lg_grp} EQUAL ${ptr_bits_min2})
      math(EXPR ndelta_limit "${g} - 1")
    else()
      set(ndelta_limit ${g})
    endif()

    while(${ndelta} LESS ${ndelta_limit} OR
          ${ndelta} EQUAL ${ndelta_limit})

      size_class(${index} ${lg_grp} ${lg_delta} ${ndelta} ${lg_p} ${lg_g} ${lg_kmax} "${output_file}")
      if(NOT "${lg_delta_lookup}" STREQUAL "no")
        math(EXPR nlbins "${index} + 1")
        # Final written value is correct:
        set(lookup_maxclass "((((size_t)1) << ${lg_grp}) + (((size_t)${ndelta}) << ${lg_delta}))")
      endif()
      if(${psz} STREQUAL "yes")
        math(EXPR npsizes "${npsizes} + 1")
      endif()
      if(NOT "${bin}" STREQUAL "no")
        math(EXPR nbins "${index} + 1")
        # # Final written value is correct:
        set(small_maxclass "((((size_t)1) << ${lg_grp}) + (((size_t)${ndelta}) << ${lg_delta}))")
        if( ${lg_g} GREATER 0)
          math(EXPR lg_large_minclass "${lg_grp} + 1")
        else()
          math(EXPR lg_large_minclass "${lg_grp} + 2")
        endif()
      endif()
      # Final written value is correct:
      set(huge_maxclass "((((size_t)1) << ${lg_grp}) + (((size_t)${ndelta}) << ${lg_delta}))")
      math(EXPR index "${index} + 1")
      math(EXPR ndelta "${ndelta} + 1")
    endwhile(${ndelta} LESS ${ndelta_limit} OR ${ndelta} EQUAL ${ndelta_limit})

    math(EXPR lg_grp "${lg_grp} + 1")
    math(EXPR lg_delta "${lg_delta} + 1")
  endwhile(${lg_grp} LESS ${ptr_bits_min1})

  file(APPEND "${output_file}" "\n")
  set(nsizes ${index})

  # Defined upon completion:
  # - ntbins
  # - nlbins
  # - nbins
  # - nsizes
  # - npsizes
  # - lg_tiny_maxclass
  # - lookup_maxclass
  # - small_maxclass
  # - lg_large_minclass
  # - huge_maxclass

  # Promote to PARENT_SCOPE
  set(ntbins ${ntbins} PARENT_SCOPE)
  set(nlbins ${nlbins} PARENT_SCOPE)
  set(nbins ${nbins} PARENT_SCOPE)
  set(nsizes ${nsizes} PARENT_SCOPE)
  set(npsizes ${npsizes} PARENT_SCOPE)
  set(lg_tiny_maxclass ${lg_tiny_maxclass} PARENT_SCOPE)
  set(lookup_maxclass ${lookup_maxclass} PARENT_SCOPE)
  set(small_maxclass ${small_maxclass} PARENT_SCOPE)
  set(lg_large_minclass ${lg_large_minclass} PARENT_SCOPE)
  set(huge_maxclass ${huge_maxclass} PARENT_SCOPE)

  # message(STATUS "size_classes_result: ntbins ${ntbins} "
          # "nlbins ${nlbins} "
          # "nbins ${nbins} "
          # "nsizes ${nsizes} "
          # "npsizes ${npsizes} "
          # "lg_tiny_maxclass ${lg_tiny_maxclass} "
          # "lookup_maxclass ${lookup_maxclass} "
          # "small_maxclass ${small_maxclass} "
          # "lg_large_minclass ${lg_large_minclass} "
          # "large_maxclass ${huge_maxclass}"
          # )

endfunction(size_classes)


###################################################
# SizeClasses
# Based on size_classes.sh
# lg_qarr - quanta
# lg_tmin - The range of tiny size classes is [2^lg_tmin..2^(lg_q-1)].
# lg_parr - list of page sizes
# lg_g - Size class group size (number of size classes for each size doubling).
function (SizeClasses lg_qarr lg_tmin lg_parr lg_g output_file)

message(STATUS "Please wait while we configure class sizes")

# message(STATUS "SizeClasses: lg_qarr:${lg_qarr} lg_tmin:${lg_tmin} lg_parr:${lg_parr} lg_g:${lg_g}\n"
# "output: ${output_file}"
# )

# The following limits are chosen such that they cover all supported platforms.
# Pointer sizes.
set(lg_zarr 2 3)
# Maximum lookup size.
set(lg_kmax 12)

file(WRITE "${output_file}"
"/* This file was automatically generated by size_classes.sh. */\n"
"/******************************************************************************/\n"
"#ifdef JEMALLOC_H_TYPES\n\n"
"/*\n"
" * This header requires LG_SIZEOF_PTR, LG_TINY_MIN, LG_QUANTUM, and LG_PAGE to\n"
" * be defined prior to inclusion, and it in turn defines:\n"
" *\n"
" *   LG_SIZE_CLASS_GROUP: Lg of size class count for each size doubling.\n"
" *   SIZE_CLASSES: Complete table of\n"
" *                 SC(index, lg_grp, lg_delta, ndelta, psz, bin, lg_delta_lookup)\n"
" *                 tuples.\n"
" *     index: Size class index.\n"
" *     lg_grp: Lg group base size (no deltas added).\n"
" *     lg_delta: Lg delta to previous size class.\n"
" *     ndelta: Delta multiplier.  size == 1<<lg_grp + ndelta<<lg_delta\n"
" *     psz: 'yes' if a multiple of the page size, 'no' otherwise.\n"
" *     bin: 'yes' if a small bin size class, 'no' otherwise.\n"
" *     lg_delta_lookup: Same as lg_delta if a lookup table size class, 'no'\n"
" *                      otherwise.\n"
" *   NTBINS: Number of tiny bins.\n"
" *   NLBINS: Number of bins supported by the lookup table.\n"
" *   NBINS: Number of small size class bins.\n"
" *   NSIZES: Number of size classes.\n"
" *   NPSIZES: Number of size classes that are a multiple of (1U << LG_PAGE).\n"
" *   LG_TINY_MAXCLASS: Lg of maximum tiny size class.\n"
" *   LOOKUP_MAXCLASS: Maximum size class included in lookup table.\n"
" *   SMALL_MAXCLASS: Maximum small size class.\n"
" *   LG_LARGE_MINCLASS: Lg of minimum large size class.\n"
" *   HUGE_MAXCLASS: Maximum (huge) size class.\n"
" */\n\n"
"#define	LG_SIZE_CLASS_GROUP	${lg_g}\n\n"
)

foreach(lg_z ${lg_zarr})
  foreach(lg_q ${lg_qarr})
    set(lg_t ${lg_tmin})
    while((${lg_t} LESS ${lg_q}) OR (${lg_t} EQUAL ${lg_q}))
      # Iterate through page sizes and compute how many bins there are.
      foreach(lg_p ${lg_parr})
        file(APPEND "${output_file}"
          "#if (LG_SIZEOF_PTR == ${lg_z} && LG_TINY_MIN == ${lg_t} && LG_QUANTUM == ${lg_q} && LG_PAGE == ${lg_p})\n"
        )
        size_classes(${lg_z} ${lg_q} ${lg_t} ${lg_p} ${lg_g} "${output_file}")
        file(APPEND "${output_file}"
          "#define	SIZE_CLASSES_DEFINED\n"
          "#define	NTBINS			${ntbins}\n"
          "#define	NLBINS			${nlbins}\n"
          "#define	NBINS			${nbins}\n"
          "#define	NSIZES			${nsizes}\n"
          "#define	NPSIZES			${npsizes}\n"
          "#define	LG_TINY_MAXCLASS	${lg_tiny_maxclass}\n"
          "#define	LOOKUP_MAXCLASS		${lookup_maxclass}\n"
          "#define	SMALL_MAXCLASS		${small_maxclass}\n"
          "#define	LG_LARGE_MINCLASS	${lg_large_minclass}\n"
          "#define	HUGE_MAXCLASS		${huge_maxclass}\n"
          "#endif\n\n"
        )
      endforeach(lg_p)
      math(EXPR lg_t "${lg_t} + 1")
    endwhile((${lg_t} LESS ${lg_q}) OR (${lg_t} EQUAL ${lg_q}))
  endforeach(lg_q in)
endforeach(lg_z)

file(APPEND "${output_file}"
"#ifndef SIZE_CLASSES_DEFINED\n"
"#  error \"No size class definitions match configuration\"\n"
"#endif\n"
"#undef SIZE_CLASSES_DEFINED\n"
"/*\n"
" * The size2index_tab lookup table uses uint8_t to encode each bin index, so we\n"
" * cannot support more than 256 small size classes.  Further constrain NBINS to\n"
" * 255 since all small size classes, plus a \"not small\" size class must be\n"
" * stored in 8 bits of arena_chunk_map_bits_t's bits field.\n"
" */\n"
"#if (NBINS > 255)\n"
"#  error \"Too many small size classes\"\n"
"#endif\n\n"
"#endif /* JEMALLOC_H_TYPES */\n"
"/******************************************************************************/\n"
"#ifdef JEMALLOC_H_STRUCTS\n\n\n"
"#endif /* JEMALLOC_H_STRUCTS */\n"
"/******************************************************************************/\n"
"#ifdef JEMALLOC_H_EXTERNS\n\n\n"
"#endif /* JEMALLOC_H_EXTERNS */\n"
"/******************************************************************************/\n"
"#ifdef JEMALLOC_H_INLINES\n\n\n"
"#endif /* JEMALLOC_H_INLINES */\n"
"/******************************************************************************/\n"
)

message(STATUS "Finished configuring class sizes\n")

endfunction (SizeClasses)

#############################################
# Read one file and append it to another
function (AppendFileContents input output)
file(READ ${input} buffer)
file(APPEND ${output} "${buffer}")
endfunction (AppendFileContents)


###############################################################
# Create a jemalloc.h header by concatenating the following headers
# Mimic processing from jemalloc.sh
# This is a Windows specific function
function (CreateJemallocHeader header_list output_file)

file(REMOVE ${output_file})

message(STATUS "Creating public header ${output_file}")

file(TO_NATIVE_PATH "${output_file}" ntv_output_file)

# File Header
file(WRITE "${ntv_output_file}"
  "#ifndef JEMALLOC_H_\n"
  "#define	JEMALLOC_H_\n"
  "#ifdef __cplusplus\n"
  "extern \"C\" {\n"
  "#endif\n\n"
)

foreach(pub_hdr ${header_list} )
  set(HDR_PATH "${JEMALLOC_BINARY_DIR}/include/jemalloc/${pub_hdr}")
  file(TO_NATIVE_PATH "${HDR_PATH}" ntv_pub_hdr)
  AppendFileContents(${ntv_pub_hdr} ${ntv_output_file})
endforeach(pub_hdr)

# Footer
file(APPEND "${ntv_output_file}"
  "#ifdef __cplusplus\n"
  "}\n"
  "#endif\n"
  "#endif /* JEMALLOC_H_ */\n"
)

endfunction(CreateJemallocHeader)


######################################################
## This function attemps to compile a one liner
# with compiler flags to append. If the compiler flags
# are supported they are appended to the variable which names
# is supplied in the APPEND_TO_VAR and the RESULT_VAR is set to
# True, otherwise to False
function(JeCflagsAppend cflags APPEND_TO_VAR RESULT_VAR)

  # Combine the result to try
  set(TFLAGS "${${APPEND_TO_VAR}} ${cflags}")
  CHECK_C_COMPILER_FLAG(${TFLAGS} status)

  if(status)
    set(${APPEND_TO_VAR} "${TFLAGS}" PARENT_SCOPE)
    set(${RESULT_VAR} True PARENT_SCOPE)
    message(STATUS "Checking whether compiler supports ${cflags} ... yes")
  else()
    set(${RESULT_VAR} False PARENT_SCOPE)
    message(STATUS "Checking whether compiler supports ${cflags} ... no")
  endif()

endfunction(JeCflagsAppend)

#############################################
# JeCompilable checks if the code supplied in the hcode
# is compilable
# label - part of the message
# hcode - code prolog such as definitions
# mcode - body of the main() function
#
# It sets rvar to yes or now depending on the result
#
# TODO: Make sure that it does expose linking problems
function (JeCompilable label hcode mcode rvar)

set(SRC
 "${hcode}

  int main(int argc, char* argv[]) {
    ${mcode}
    return 0;
  }")

  # We may want a stronger check here
  CHECK_C_SOURCE_COMPILES("${SRC}" status)

  if(status)
    set(${rvar} True PARENT_SCOPE)
    message(STATUS "whether ${label} is compilable ... yes")
  else()
    set(${rvar} False PARENT_SCOPE)
    message(STATUS "whether ${label} is compilable ... no")
  endif()

endfunction(JeCompilable)
