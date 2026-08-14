-- arrayRemove with tuples containing NULL should not cause a logical error
SELECT arrayRemove([(255, -2)], (NULL, 1));
SELECT arrayRemove([(1, 2), (3, 4)], (NULL, 2));
SELECT arrayRemove([(1, NULL)], (1, NULL));
SELECT arrayRemove([(toNullable(1), 2)], (NULL, 2));
