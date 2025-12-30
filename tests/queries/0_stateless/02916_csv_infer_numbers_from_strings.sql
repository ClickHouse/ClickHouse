set input_format_csv_try_infer_numbers_from_strings=1;
desc format(CSV, '"42","42.42","True"');
desc format(CSV, '"42","42.42","True"\n"abc","def","ghk"');

