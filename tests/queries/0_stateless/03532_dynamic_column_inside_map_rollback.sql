select c0 from format(CSV, 'c0 Map(Dynamic, String)', $$
"{}"
"{['a', 'b'] : 'a', ''a' : 1}"
$$) settings input_format_allow_errors_num=1;

