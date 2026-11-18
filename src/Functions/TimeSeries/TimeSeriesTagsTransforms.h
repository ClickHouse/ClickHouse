#pragma once

#include <Core/Types.h>
#include <Interpreters/ContextTimeSeriesTagsCollector.h>
#include <Common/VectorWithMemoryTracking.h>


/// Transformations of sets of tags. Each function transforms every element of a vector in place.
/// Each input set of tags must be sorted by tag name and must not contain tags with empty values,
/// the transformed sets of tags keep these properties.
namespace DB::TimeSeriesTagsTransforms
{
using TagNamesAndValues = ContextTimeSeriesTagsCollector::TagNamesAndValues;
using TagNamesAndValuesPtr = ContextTimeSeriesTagsCollector::TagNamesAndValuesPtr;

/// Removes a specified tag from each set of tags.
void removeTag(VectorWithMemoryTracking<TagNamesAndValuesPtr> & tags_vector, const String & tag_to_remove);

/// Removes specified tags from each set of tags.
void removeTags(VectorWithMemoryTracking<TagNamesAndValuesPtr> & tags_vector, const Strings & tags_to_remove);

/// Removes all tags except specified ones from each set of tags.
void removeAllTagsExcept(VectorWithMemoryTracking<TagNamesAndValuesPtr> & tags_vector, const Strings & tags_to_keep);

/// Copies a specified tag from `src_tags_vector[i]` to `dest_tags_vector[i]`, replacing any previous value of the copied tag.
/// If the copied tag isn't present in `src_tags_vector[i]` then it's removed from `dest_tags_vector[i]` as well.
/// Both vectors must have the same size.
void copyTag(
    VectorWithMemoryTracking<TagNamesAndValuesPtr> & dest_tags_vector,
    const VectorWithMemoryTracking<TagNamesAndValuesPtr> & src_tags_vector,
    const String & tag_to_copy);

/// Copies specified tags from `src_tags_vector[i]` to `dest_tags_vector[i]`, replacing any previous values of the copied tags.
/// If some of the copied tags aren't present in `src_tags_vector[i]` then they're removed from `dest_tags_vector[i]` as well.
/// Both vectors must have the same size.
void copyTags(
    VectorWithMemoryTracking<TagNamesAndValuesPtr> & dest_tags_vector,
    const VectorWithMemoryTracking<TagNamesAndValuesPtr> & src_tags_vector,
    const Strings & tags_to_copy);

/// Joins all the values of all the `src_tags` using `separator` and sets the tag `dest_tag` to the joined value in each set of tags.
/// This function implements the logic of promql function label_join().
void joinTags(
    VectorWithMemoryTracking<TagNamesAndValuesPtr> & tags_vector,
    const String & dest_tag,
    const String & separator,
    const Strings & src_tags);

/// Matches the regular expression `regex` against the value of the tag `src_tag` in each set of tags.
/// If it matches, the value of the tag `dest_tag` in the result will be the expansion of `replacement`,
/// together with the original tags in the input.
/// Capturing groups in the regular expression can be referenced with $1, $2, etc.
/// Named capturing groups in the regular expression can be referenced with $name (where name is the capturing group name).
/// If the regular expression doesn't match then the set of tags is left unchanged.
/// This function implements the logic of promql function label_replace().
void replaceTag(
    VectorWithMemoryTracking<TagNamesAndValuesPtr> & tags_vector,
    const String & dest_tag,
    const String & replacement,
    const String & src_tag,
    const String & regex);

}
