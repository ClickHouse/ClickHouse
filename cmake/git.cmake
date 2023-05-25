find_package(Git)

# Make basic Git information available as variables. Such data will later be embedded into the build, e.g. for view SYSTEM.BUILD_OPTIONS.
if (Git_FOUND)
  # Commit hash + whether the building workspace was dirty or not
  execute_process(COMMAND
    "${GIT_EXECUTABLE}" rev-parse HEAD
    WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
    OUTPUT_VARIABLE GIT_HASH
    ERROR_QUIET OUTPUT_STRIP_TRAILING_WHITESPACE)

  # Branch name
  execute_process(COMMAND
    "${GIT_EXECUTABLE}" rev-parse --abbrev-ref HEAD
    WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
    OUTPUT_VARIABLE GIT_BRANCH
    ERROR_QUIET OUTPUT_STRIP_TRAILING_WHITESPACE)

  # Date of the commit
  SET(ENV{TZ} "UTC")
  execute_process(COMMAND
    "${GIT_EXECUTABLE}" log -1 --format=%ad --date=iso-local
    WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
    OUTPUT_VARIABLE GIT_DATE
    ERROR_QUIET OUTPUT_STRIP_TRAILING_WHITESPACE)

  # Subject of the commit
  execute_process(COMMAND
    "${GIT_EXECUTABLE}" log -1 --format=%s
    WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
    OUTPUT_VARIABLE GIT_COMMIT_SUBJECT
    ERROR_QUIET OUTPUT_STRIP_TRAILING_WHITESPACE)

  message(STATUS "Git HEAD commit hash: ${GIT_HASH}")

  execute_process(
    COMMAND ${GIT_EXECUTABLE} status
    WORKING_DIRECTORY ${PROJECT_SOURCE_DIR} OUTPUT_STRIP_TRAILING_WHITESPACE)
else()
  message(STATUS "Git could not be found.")
endif()

