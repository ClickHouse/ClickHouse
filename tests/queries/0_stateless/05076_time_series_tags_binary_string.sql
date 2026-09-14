SELECT 'timeSeriesTagsToGroup binary String tags:';

SELECT string_array_group != fixed_group,
       string_map_group != fixed_group,
       string_array_group == string_map_group,
       fixed_group == plain_group
FROM
(
    SELECT timeSeriesTagsToGroup([('tag', concat('value', unhex('00')))]) AS string_array_group,
           timeSeriesTagsToGroup(map('tag', concat('value', unhex('00')))) AS string_map_group,
           timeSeriesTagsToGroup([('tag', toFixedString('value', 6))]) AS fixed_group,
           timeSeriesTagsToGroup([('tag', 'value')]) AS plain_group
);
