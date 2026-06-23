add_library(_wagyu INTERFACE)
target_include_directories(_wagyu SYSTEM BEFORE INTERFACE ${ClickHouse_SOURCE_DIR}/contrib/wagyu/include)
# wagyu's headers include <mapbox/geometry/{point,box,polygon,...}.hpp> from mapbox-geometry.
target_link_libraries(_wagyu INTERFACE ch_contrib::mapbox_geometry)
add_library(ch_contrib::wagyu ALIAS _wagyu)
