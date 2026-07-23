export const ScalePlanFeatureBadge = ({feature='此功能', linking_verb_are = false}) => {
    return (
        <div className="scalePlanFeatureContainer">
            <div className="scalePlanFeatureBadge">
                Scale 套餐功能
            </div>
            <div>
                <p>{feature} {linking_verb_are ? '可在' : '可在'} Scale 和 Enterprise 套餐中使用。要升级，请访问 Cloud Console 中的套餐页面。</p>
            </div>
        </div>
    )
}