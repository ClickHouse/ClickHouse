SELECT count() > 0 FROM system.settings WHERE tier = 'Production';
SELECT count() > 0 FROM system.settings WHERE tier = 'Beta';
SELECT count() > 0 FROM system.settings WHERE tier = 'Experimental';
SELECT count() > 0 FROM system.settings WHERE tier = 'PrivatePreview';
SELECT count() > 0 FROM system.settings WHERE tier = 'Obsolete';
SELECT count() == countIf(tier IN ['Production', 'Beta', 'PrivatePreview', 'Experimental', 'Obsolete']) FROM system.settings;

SELECT count() > 0 FROM system.merge_tree_settings WHERE tier = 'Production';
SELECT count() > 0 FROM system.merge_tree_settings WHERE tier = 'Beta';
SELECT count() > 0 FROM system.merge_tree_settings WHERE tier = 'Experimental';
SELECT count() == 0 FROM system.merge_tree_settings WHERE tier = 'PrivatePreview';
SELECT count() > 0 FROM system.merge_tree_settings WHERE tier = 'Obsolete';
SELECT count() == countIf(tier IN ['Production', 'Beta', 'PrivatePreview', 'Experimental', 'Obsolete']) FROM system.merge_tree_settings;
