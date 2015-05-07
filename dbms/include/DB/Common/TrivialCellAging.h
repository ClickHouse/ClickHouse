#pragma once

#include <DB/Common/BaseCellAging.h>

namespace DB
{

/** Класс производный от BaseCellAging для тех случаев, когда на самом деле кэш не нуждается
  * в управлении временем жизнью своих элементов. Ничего не делает.
  */
class TrivialCellAging final : public BaseCellAging<char, char>
{
public:
	const Timestamp & update() override { return timestamp; }
	bool expired(const Timestamp & last_timestamp, const Delay & expiration_delay) const override { return true; }
};

}
