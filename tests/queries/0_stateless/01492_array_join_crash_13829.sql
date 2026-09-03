SELECT NULL = countEqual(materialize([arrayJoin([NULL, NULL, NULL]), NULL AS x, arrayJoin([255, 1025, NULL, NULL]), arrayJoin([2, 1048576, NULL, NULL])]), materialize(x)) format Null;
