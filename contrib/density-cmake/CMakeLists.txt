SET (USE_INTERNAL_DENSITY_LIBRARY 1)
SET (LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/density")

# cd contrib/density
# find . -name '*.c' | grep -vP 'deprecated|legacy' | sort | sed 's/^\./        ${LIBRARY_DIR}/'
SET (Sources
        ${LIBRARY_DIR}/src/algorithms/algorithms.c
        ${LIBRARY_DIR}/src/algorithms/chameleon/core/chameleon_decode.c
        ${LIBRARY_DIR}/src/algorithms/chameleon/core/chameleon_encode.c
        ${LIBRARY_DIR}/src/algorithms/cheetah/core/cheetah_decode.c
        ${LIBRARY_DIR}/src/algorithms/cheetah/core/cheetah_encode.c
        ${LIBRARY_DIR}/src/algorithms/dictionaries.c
        ${LIBRARY_DIR}/src/algorithms/lion/core/lion_decode.c
        ${LIBRARY_DIR}/src/algorithms/lion/core/lion_encode.c
        ${LIBRARY_DIR}/src/algorithms/lion/forms/lion_form_model.c
        ${LIBRARY_DIR}/src/buffers/buffer.c
        ${LIBRARY_DIR}/src/globals.c
        ${LIBRARY_DIR}/src/structure/header.c
)

# cd contrib/density
# find . -name '*.h' | grep -vP 'deprecated|legacy' | sort | sed 's/^\./        ${LIBRARY_DIR}/'
SET (Headers
        ${LIBRARY_DIR}/src/algorithms/algorithms.h
        ${LIBRARY_DIR}/src/algorithms/chameleon/chameleon.h
        ${LIBRARY_DIR}/src/algorithms/chameleon/core/chameleon_decode.h
        ${LIBRARY_DIR}/src/algorithms/chameleon/core/chameleon_encode.h
        ${LIBRARY_DIR}/src/algorithms/chameleon/dictionary/chameleon_dictionary.h
        ${LIBRARY_DIR}/src/algorithms/cheetah/cheetah.h
        ${LIBRARY_DIR}/src/algorithms/cheetah/core/cheetah_decode.h
        ${LIBRARY_DIR}/src/algorithms/cheetah/core/cheetah_encode.h
        ${LIBRARY_DIR}/src/algorithms/cheetah/dictionary/cheetah_dictionary.h
        ${LIBRARY_DIR}/src/algorithms/dictionaries.h
        ${LIBRARY_DIR}/src/algorithms/lion/core/lion_decode.h
        ${LIBRARY_DIR}/src/algorithms/lion/core/lion_encode.h
        ${LIBRARY_DIR}/src/algorithms/lion/dictionary/lion_dictionary.h
        ${LIBRARY_DIR}/src/algorithms/lion/forms/lion_form_model.h
        ${LIBRARY_DIR}/src/algorithms/lion/lion.h
        ${LIBRARY_DIR}/src/buffers/buffer.h
        ${LIBRARY_DIR}/src/density_api.h
        ${LIBRARY_DIR}/src/globals.h
        ${LIBRARY_DIR}/src/structure/header.h
)

ADD_LIBRARY(density ${Sources} ${Headers})

target_include_directories (density PUBLIC ${LIBRARY_DIR})
