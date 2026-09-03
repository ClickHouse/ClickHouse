/* char function */
SELECT char(65, 66.1, 67.2, 68.3, 97.4, 98.5, 99.6, 100.7, 101.0, 102.0, 103.0);
SELECT char(65 + 256, 66 + 1024, 66 + 1024 + 1);
SELECT char(65, 66 + number, 67 + number) from numbers(3);
