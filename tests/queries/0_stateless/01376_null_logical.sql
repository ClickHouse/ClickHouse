SELECT NULL OR 1;
SELECT materialize(NULL) OR materialize(1);

SELECT NULL AND 0;
SELECT materialize(NULL) AND materialize(0);

SELECT NULL OR 0;
SELECT materialize(NULL) OR materialize(0);

SELECT NULL AND 1;
SELECT materialize(NULL) AND materialize(1);
