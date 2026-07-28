option(ENABLE_S2_GEOMETRY "Enable S2 Geometry" ${ENABLE_LIBRARIES})

if (NOT ENABLE_S2_GEOMETRY)
    message(STATUS "Not using S2 Geometry")
    return()
endif()

set(S2_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/s2geometry/src")

set(S2_SRCS
    "${S2_SOURCE_DIR}/s2/encoded_s2cell_id_vector.cc"
    "${S2_SOURCE_DIR}/s2/encoded_s2point_vector.cc"
    "${S2_SOURCE_DIR}/s2/encoded_s2shape_index.cc"
    "${S2_SOURCE_DIR}/s2/encoded_string_vector.cc"
    "${S2_SOURCE_DIR}/s2/id_set_lexicon.cc"
    "${S2_SOURCE_DIR}/s2/mutable_s2shape_index.cc"
    "${S2_SOURCE_DIR}/s2/r2rect.cc"
    "${S2_SOURCE_DIR}/s2/s1angle.cc"
    "${S2_SOURCE_DIR}/s2/s1chord_angle.cc"
    "${S2_SOURCE_DIR}/s2/s1interval.cc"
    "${S2_SOURCE_DIR}/s2/s2boolean_operation.cc"
    "${S2_SOURCE_DIR}/s2/s2buffer_operation.cc"
    "${S2_SOURCE_DIR}/s2/s2builder.cc"
    "${S2_SOURCE_DIR}/s2/s2builder_graph.cc"
    "${S2_SOURCE_DIR}/s2/s2builderutil_closed_set_normalizer.cc"
    "${S2_SOURCE_DIR}/s2/s2builderutil_find_polygon_degeneracies.cc"
    "${S2_SOURCE_DIR}/s2/s2builderutil_get_snapped_winding_delta.cc"
    "${S2_SOURCE_DIR}/s2/s2builderutil_lax_polygon_layer.cc"
    "${S2_SOURCE_DIR}/s2/s2builderutil_lax_polyline_layer.cc"
    "${S2_SOURCE_DIR}/s2/s2builderutil_s2point_vector_layer.cc"
    "${S2_SOURCE_DIR}/s2/s2builderutil_s2polygon_layer.cc"
    "${S2_SOURCE_DIR}/s2/s2builderutil_s2polyline_layer.cc"
    "${S2_SOURCE_DIR}/s2/s2builderutil_s2polyline_vector_layer.cc"
    "${S2_SOURCE_DIR}/s2/s2builderutil_snap_functions.cc"
    "${S2_SOURCE_DIR}/s2/s2cap.cc"
    "${S2_SOURCE_DIR}/s2/s2cell.cc"
    "${S2_SOURCE_DIR}/s2/s2cell_id.cc"
    "${S2_SOURCE_DIR}/s2/s2cell_index.cc"
    "${S2_SOURCE_DIR}/s2/s2cell_union.cc"
    "${S2_SOURCE_DIR}/s2/s2centroids.cc"
    "${S2_SOURCE_DIR}/s2/s2chain_interpolation_query.cc"
    "${S2_SOURCE_DIR}/s2/s2closest_cell_query.cc"
    "${S2_SOURCE_DIR}/s2/s2closest_edge_query.cc"
    "${S2_SOURCE_DIR}/s2/s2closest_point_query.cc"
    "${S2_SOURCE_DIR}/s2/s2contains_vertex_query.cc"
    "${S2_SOURCE_DIR}/s2/s2convex_hull_query.cc"
    "${S2_SOURCE_DIR}/s2/s2coords.cc"
    "${S2_SOURCE_DIR}/s2/s2crossing_edge_query.cc"
    "${S2_SOURCE_DIR}/s2/s2debug.cc"
    "${S2_SOURCE_DIR}/s2/s2density_tree.cc"
    "${S2_SOURCE_DIR}/s2/s2earth.cc"
    "${S2_SOURCE_DIR}/s2/s2edge_clipping.cc"
    "${S2_SOURCE_DIR}/s2/s2edge_crosser.cc"
    "${S2_SOURCE_DIR}/s2/s2edge_crossings.cc"
    "${S2_SOURCE_DIR}/s2/s2edge_distances.cc"
    "${S2_SOURCE_DIR}/s2/s2edge_tessellator.cc"
    "${S2_SOURCE_DIR}/s2/s2error.cc"
    "${S2_SOURCE_DIR}/s2/s2fractal.cc"
    "${S2_SOURCE_DIR}/s2/s2furthest_edge_query.cc"
    "${S2_SOURCE_DIR}/s2/s2hausdorff_distance_query.cc"
    "${S2_SOURCE_DIR}/s2/s2index_cell_data.cc"
    "${S2_SOURCE_DIR}/s2/s2latlng.cc"
    "${S2_SOURCE_DIR}/s2/s2latlng_rect.cc"
    "${S2_SOURCE_DIR}/s2/s2latlng_rect_bounder.cc"
    "${S2_SOURCE_DIR}/s2/s2lax_loop_shape.cc"
    "${S2_SOURCE_DIR}/s2/s2lax_polygon_shape.cc"
    "${S2_SOURCE_DIR}/s2/s2lax_polyline_shape.cc"
    "${S2_SOURCE_DIR}/s2/s2loop.cc"
    "${S2_SOURCE_DIR}/s2/s2loop_measures.cc"
    "${S2_SOURCE_DIR}/s2/s2max_distance_targets.cc"
    "${S2_SOURCE_DIR}/s2/s2measures.cc"
    "${S2_SOURCE_DIR}/s2/s2memory_tracker.cc"
    "${S2_SOURCE_DIR}/s2/s2metrics.cc"
    "${S2_SOURCE_DIR}/s2/s2min_distance_targets.cc"
    "${S2_SOURCE_DIR}/s2/s2padded_cell.cc"
    "${S2_SOURCE_DIR}/s2/s2point_compression.cc"
    "${S2_SOURCE_DIR}/s2/s2point_region.cc"
    "${S2_SOURCE_DIR}/s2/s2pointutil.cc"
    "${S2_SOURCE_DIR}/s2/s2polygon.cc"
    "${S2_SOURCE_DIR}/s2/s2polyline.cc"
    "${S2_SOURCE_DIR}/s2/s2polyline_alignment.cc"
    "${S2_SOURCE_DIR}/s2/s2polyline_measures.cc"
    "${S2_SOURCE_DIR}/s2/s2polyline_simplifier.cc"
    "${S2_SOURCE_DIR}/s2/s2predicates.cc"
    "${S2_SOURCE_DIR}/s2/s2projections.cc"
    "${S2_SOURCE_DIR}/s2/s2r2rect.cc"
    "${S2_SOURCE_DIR}/s2/s2random.cc"
    "${S2_SOURCE_DIR}/s2/s2region_coverer.cc"
    "${S2_SOURCE_DIR}/s2/s2region_intersection.cc"
    "${S2_SOURCE_DIR}/s2/s2region_sharder.cc"
    "${S2_SOURCE_DIR}/s2/s2region_term_indexer.cc"
    "${S2_SOURCE_DIR}/s2/s2region_union.cc"
    "${S2_SOURCE_DIR}/s2/s2shape_index.cc"
    "${S2_SOURCE_DIR}/s2/s2shape_index_buffered_region.cc"
    "${S2_SOURCE_DIR}/s2/s2shape_index_measures.cc"
    "${S2_SOURCE_DIR}/s2/s2shape_measures.cc"
    "${S2_SOURCE_DIR}/s2/s2shape_nesting_query.cc"
    "${S2_SOURCE_DIR}/s2/s2shapeutil_build_polygon_boundaries.cc"
    "${S2_SOURCE_DIR}/s2/s2shapeutil_coding.cc"
    "${S2_SOURCE_DIR}/s2/s2shapeutil_contains_brute_force.cc"
    "${S2_SOURCE_DIR}/s2/s2shapeutil_conversion.cc"
    "${S2_SOURCE_DIR}/s2/s2shapeutil_count_vertices.cc"
    "${S2_SOURCE_DIR}/s2/s2shapeutil_edge_iterator.cc"
    "${S2_SOURCE_DIR}/s2/s2shapeutil_edge_wrap.cc"
    "${S2_SOURCE_DIR}/s2/s2shapeutil_get_reference_point.cc"
    "${S2_SOURCE_DIR}/s2/s2shapeutil_visit_crossing_edge_pairs.cc"
    "${S2_SOURCE_DIR}/s2/s2testing.cc"
    "${S2_SOURCE_DIR}/s2/s2text_format.cc"
    "${S2_SOURCE_DIR}/s2/s2wedge_relations.cc"
    "${S2_SOURCE_DIR}/s2/s2winding_operation.cc"
    "${S2_SOURCE_DIR}/s2/util/bits/bit-interleave.cc"
    "${S2_SOURCE_DIR}/s2/util/coding/coder.cc"
    "${S2_SOURCE_DIR}/s2/util/coding/varint.cc"
    "${S2_SOURCE_DIR}/s2/util/math/exactfloat/exactfloat.cc"
    "${S2_SOURCE_DIR}/s2/util/math/mathutil.cc"
    "${S2_SOURCE_DIR}/s2/util/units/length-units.cc"
)

add_library(_s2 ${S2_SRCS})
add_library(ch_contrib::s2 ALIAS _s2)

if (TARGET OpenSSL::SSL)
    target_link_libraries(_s2 PRIVATE OpenSSL::Crypto OpenSSL::SSL)
endif()

# Copied from contrib/s2geometry/CMakeLists
target_link_libraries(_s2 PRIVATE
    absl::base
    absl::btree
    absl::check
    absl::config
    absl::core_headers
    absl::dynamic_annotations
    absl::endian
    absl::fixed_array
    absl::flags
    absl::flat_hash_map
    absl::flat_hash_set
    absl::hash
    absl::inlined_vector
    absl::int128
    absl::log
    absl::log_severity
    absl::memory
    absl::span
    absl::status
    absl::str_format
    absl::strings
    absl::type_traits
    absl::utility
    absl::vlog_is_on
)

target_include_directories(_s2 SYSTEM BEFORE PUBLIC "${S2_SOURCE_DIR}/")
target_include_directories(_s2 SYSTEM PUBLIC "${ABSL_SOURCE_DIR}")
