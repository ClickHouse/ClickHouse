SELECT throwIf(
    (SELECT value FROM system.build_options WHERE name = 'SYSTEM')
    != splitByChar(' ', getOSKernelVersion())[1],
    'SYSTEM build option does not match kernel version'
)
