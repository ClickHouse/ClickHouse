SELECT
  c1,
  range(0, c1) AS zero_as_start_val,
  range(1, c1) AS one_as_start_val,
  range(c1) AS no_start_val,
  range(c1, c1 * 2) AS val_as_start,
  range(c1, c1 * c1, c1) AS complex_start_step
FROM values(1, 2, 3, 4, 5)
FORMAT Vertical;
