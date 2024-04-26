#pragma once

#include <Common/TypePromotion.h>

namespace DB
{

class IStreamSubscription : public TypePromotion<IStreamSubscription>
{
public:
  virtual ~IStreamSubscription() = default;
};

using StreamSubscriptionPtr = std::shared_ptr<IStreamSubscription>;
using StreamSubscriptionWeakPtr = std::weak_ptr<IStreamSubscription>;

}
