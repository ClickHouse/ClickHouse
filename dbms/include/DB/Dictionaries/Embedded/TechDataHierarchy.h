#pragma once

#include <common/singleton.h>
#include <common/Common.h>


/** @brief Класс, позволяющий узнавать, принадлежит ли поисковая система или операционная система
  * другой поисковой или операционной системе, соответственно.
  * Информацию об иерархии регионов загружает из БД.
  */
class TechDataHierarchy
{
private:
	UInt8 os_parent[256] {};
	UInt8 se_parent[256] {};

public:
	void reload();

	/// Has corresponding section in configuration file.
	static bool isConfigured();


	/// Отношение "принадлежит".
	bool isOSIn(UInt8 lhs, UInt8 rhs) const
	{
		while (lhs != rhs && os_parent[lhs])
			lhs = os_parent[lhs];

		return lhs == rhs;
	}

	bool isSEIn(UInt8 lhs, UInt8 rhs) const
	{
		while (lhs != rhs && se_parent[lhs])
			lhs = se_parent[lhs];

		return lhs == rhs;
	}


	UInt8 OSToParent(UInt8 x) const
	{
		return os_parent[x];
	}

	UInt8 SEToParent(UInt8 x) const
	{
		return se_parent[x];
	}


	/// К самому верхнему предку.
	UInt8 OSToMostAncestor(UInt8 x) const
	{
		while (os_parent[x])
			x = os_parent[x];
		return x;
	}

	UInt8 SEToMostAncestor(UInt8 x) const
	{
		while (se_parent[x])
			x = se_parent[x];
		return x;
	}
};


class TechDataHierarchySingleton : public Singleton<TechDataHierarchySingleton>, public TechDataHierarchy {};
