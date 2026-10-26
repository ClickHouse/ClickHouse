export const EnterprisePlanFeatureBadge = ({feature='此功能', support=false, linking_verb_are = false}) => {
    return (
        <div className="enterprisePlanFeatureContainer">
            <div className="enterprisePlanFeatureBadge">
                Enterprise 套餐功能
            </div>
            <div>
                <p>{feature} {linking_verb_are ? '在' : '在'}Enterprise 套餐中可用。{support ? `如需启用此功能，请联系支持团队。` : '如需升级，请访问 Cloud Console 中的套餐页面。'}</p>
            </div>
        </div>
    )
}