# Print the status of the git repository (if git is available).
# This is useful for troubleshooting build failure reports
find_package(Git)

if (Git_FOUND)
  execute_process(
    COMMAND ${GIT_EXECUTABLE} rev-parse HEAD
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
    OUTPUT_VARIABLE GIT_COMMIT_ID
    OUTPUT_STRIP_TRAILING_WHITESPACE)
  message(STATUS "HEAD's commit hash ${GIT_COMMIT_ID}")
  execute_process(
    COMMAND ${GIT_EXECUTABLE} status
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
else()
  message(STATUS "The git program could not be found.")
endif()
