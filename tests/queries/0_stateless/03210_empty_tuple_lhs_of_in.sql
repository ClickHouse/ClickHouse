SELECT tuple() IN tuple(1) SETTINGS allow_experimental_map_type = 1; -- { serverError INCORRECT_ELEMENT_OF_SET }
