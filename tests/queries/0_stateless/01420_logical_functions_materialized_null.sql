SELECT NULL AND 1;
SELECT NULL OR 1;
SELECT materialize(NULL) AND 1;
SELECT materialize(NULL) OR 1;
SELECT arrayJoin([NULL]) AND 1;
SELECT arrayJoin([NULL]) OR 1;

SELECT isConstant(arrayJoin([NULL]) AND 1);
