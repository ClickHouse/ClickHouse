<yandex>

<storage_configuration>
    <disks>
        <default>
            <keep_free_space_bytes>1024</keep_free_space_bytes>
        </default>
        <jbod1>
            <path>/jbod1/</path>
        </jbod1>
        <jbod2>
            <path>/jbod2/</path>
            <keep_free_space_bytes>10485760</keep_free_space_bytes>
                                 <!-- 10MB -->
        </jbod2>
        <external>
            <path>/external/</path>
        </external>
    </disks>

    <policies>
        <small_jbod_with_external>
            <volumes>
                <main>
                    <disk>jbod1</disk>
                </main>
                <external>
                    <disk>external</disk>
                </external>
            </volumes>
        </small_jbod_with_external>

        <one_more_small_jbod_with_external>
            <volumes>
                <m>
                    <disk>jbod1</disk>
                </m>
                <e>
                    <disk>external</disk>
                </e>
            </volumes>
        </one_more_small_jbod_with_external>

        <!-- store on JBOD by default (round-robin), store big parts on external -->
        <jbods_with_external>
            <volumes>
                <main>
                    <disk>jbod1</disk>
                    <disk>jbod2</disk>
                    <max_data_part_size_bytes>10485760</max_data_part_size_bytes>
                                            <!-- 10MB -->
                </main>
                <external>
                    <disk>external</disk>
                </external>
            </volumes>
        </jbods_with_external>

        <!-- Moving all parts jbod1 if acquired more than 70% -->
        <moving_jbod_with_external>
            <volumes>
                <main>
                    <disk>jbod1</disk>
                </main>
                <external>
                    <disk>external</disk>
                </external>
            </volumes>
            <move_factor>0.7</move_factor>
        </moving_jbod_with_external>

        <!-- store local by default, store big parts on external -->
        <default_disk_with_external>
            <volumes>
                <small>
                    <disk>default</disk>
                    <max_data_part_size_bytes>2097152</max_data_part_size_bytes>
                                            <!-- 2MB -->
                </small>
                <big>
                    <disk>external</disk>
                    <max_data_part_size_bytes>20971520</max_data_part_size_bytes>
                                            <!-- 20MB -->
                </big>
            </volumes>
        </default_disk_with_external>

        <!-- special policy for checking validation of `max_data_part_size` -->
        <special_warning_policy>
            <volumes>
                <special_warning_zero_volume>
                    <disk>default</disk>
                    <max_data_part_size_bytes>0</max_data_part_size_bytes>
                </special_warning_zero_volume>
                <special_warning_default_volume>
                    <disk>external</disk>
                </special_warning_default_volume>
                <special_warning_small_volume>
                    <disk>jbod1</disk>
                    <max_data_part_size_bytes>1024</max_data_part_size_bytes>
                </special_warning_small_volume>
                <special_warning_big_volume>
                    <disk>jbod2</disk>
                    <max_data_part_size_bytes>1024000000</max_data_part_size_bytes>
                </special_warning_big_volume>
            </volumes>
        </special_warning_policy>

    </policies>

</storage_configuration>

</yandex>
