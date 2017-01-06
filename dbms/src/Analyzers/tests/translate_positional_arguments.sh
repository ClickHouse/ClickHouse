#!/bin/sh

echo "SELECT abc, def + 1, count() GROUP BY 1, 2 ORDER BY 1 DESC" | ./translate_positional_arguments
