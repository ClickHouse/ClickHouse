SELECT if(if(dummy % NULL, NULL, if(dummy % NULL, 1, 2)), 1, toFloat64(dummy)) SETTINGS optimize_if_chain_to_multiif = 1;
