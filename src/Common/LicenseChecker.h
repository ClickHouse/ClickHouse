#pragma once

#include "config.h"

#ifdef USE_LICENSE_PUBLIC_KEY

#    include <memory>
#    include <base/types.h>

namespace DB
{

class BackgroundSchedulePoolTaskHolder;

class LicenseChecker
{
private:
    std::unique_ptr<BackgroundSchedulePoolTaskHolder> check_task;

    LicenseChecker();

    void checkLicenseRoutine();

    std::atomic<bool> licenseValid;

    void checkAndSetLicenseValidity(String LicenseKey);

public:
    static LicenseChecker & getInstance();
    LicenseChecker(const LicenseChecker &) = delete;
    LicenseChecker & operator=(const LicenseChecker &) = delete;

    bool isLicenseValid() { return licenseValid.load(); }
};

};

#endif
