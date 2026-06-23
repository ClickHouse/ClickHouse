add_library(_mapbox_geometry INTERFACE)
target_include_directories(_mapbox_geometry SYSTEM BEFORE INTERFACE ${ClickHouse_SOURCE_DIR}/contrib/mapbox-geometry/include)
add_library(ch_contrib::mapbox_geometry ALIAS _mapbox_geometry)
