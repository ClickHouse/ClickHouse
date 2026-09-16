export const EnterprisePlanFeatureBadge = ({feature='此功能', support=false, linking_verb_are = false}) => {
    return (
        <div className="enterprisePlanFeatureContainer">
            <div className="enterprisePlanFeatureBadge">
                Enterprise 计划功能
            </div>
            <div>
                <p>{feature} {linking_verb_are ? '可在' : '可在'} Enterprise 计划中使用。{support ? `请联系支持团队以启用此功能。` : '如需升级，请前往 Cloud Console 的套餐页面。'}</p>
            </div>
        </div>
    )
}