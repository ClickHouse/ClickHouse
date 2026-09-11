export const ScalePlanFeatureBadge = ({feature='此功能', linking_verb_are = false}) => {
    return (
        <div className="scalePlanFeatureContainer">
            <div className="scalePlanFeatureBadge">
                Scale 计划功能
            </div>
            <div>
                <p>{feature} {linking_verb_are ? '仅在' : '仅在'} Scale 和 Enterprise 计划中可用。要升级，请访问 Cloud Console 中的计划页面。</p>
            </div>
        </div>
    )
}