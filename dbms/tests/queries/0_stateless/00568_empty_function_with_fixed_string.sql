SELECT toFixedString('', 4) AS str, empty(str) AS is_empty;
SELECT toFixedString('\0abc', 4) AS str, empty(str) AS is_empty;
